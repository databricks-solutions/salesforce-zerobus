package pubsub

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/databricks-solutions/go-salesforce-zerobus/internal/backoff"
	"github.com/databricks-solutions/go-salesforce-zerobus/internal/config"
	"github.com/databricks-solutions/go-salesforce-zerobus/internal/sfauth"
	pb "github.com/databricks-solutions/go-salesforce-zerobus/proto/gen"
)

// CDCEvent is the decoded Salesforce Change Data Capture event.
type CDCEvent struct {
	EventID        string
	SchemaID       string
	ReplayID       string // hex-encoded
	ChangeType     string
	EntityName     string
	ChangeOrigin   string
	RecordIDs      []string
	ChangedFields  []string
	NulledFields   []string
	DiffFields     []string
	RecordDataJSON string
	PayloadBinary  []byte
	SchemaJSON     string
	OrgID          string
}

// SubscriberClient manages the gRPC connection to Salesforce Pub/Sub API.
type SubscriberClient struct {
	conn    *grpc.ClientConn
	stub    pb.PubSubClient
	creds   *sfauth.Credentials
	cfg     *config.Config
	schemas *SchemaCache
	dedup   *DedupCache
	logger  *slog.Logger

	// Re-authentication function for token refresh
	reauth func(ctx context.Context) (*sfauth.Credentials, error)
}

// NewSubscriberClient creates a new Salesforce Pub/Sub subscriber.
func NewSubscriberClient(
	cfg *config.Config,
	creds *sfauth.Credentials,
	logger *slog.Logger,
	reauth func(ctx context.Context) (*sfauth.Credentials, error),
) (*SubscriberClient, error) {
	conn, err := dialGRPC(cfg.GRPCAddress())
	if err != nil {
		return nil, fmt.Errorf("dialing gRPC: %w", err)
	}

	dedup, err := NewDedupCache(10000)
	if err != nil {
		return nil, fmt.Errorf("creating dedup cache: %w", err)
	}

	return &SubscriberClient{
		conn:    conn,
		stub:    pb.NewPubSubClient(conn),
		creds:   creds,
		cfg:     cfg,
		schemas: NewSchemaCache(logger),
		dedup:   dedup,
		logger:  logger,
		reauth:  reauth,
	}, nil
}

func dialGRPC(addr string) (*grpc.ClientConn, error) {
	tlsCreds := credentials.NewTLS(&tls.Config{})

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(tlsCreds),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                60 * time.Second,
			Timeout:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.WithUnaryInterceptor(traceInterceptor),
		grpc.WithStreamInterceptor(traceStreamInterceptor),
	}

	return grpc.NewClient(addr, opts...)
}

// Trace interceptors add a unique client trace ID to each request.
func traceInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	ctx = metadata.AppendToOutgoingContext(ctx, "x-client-trace-id", uuid.New().String())
	return invoker(ctx, method, req, reply, cc, opts...)
}

func traceStreamInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	ctx = metadata.AppendToOutgoingContext(ctx, "x-client-trace-id", uuid.New().String())
	return streamer(ctx, desc, cc, method, opts...)
}

// Run is the main subscriber loop. It writes decoded events to eventCh.
// It waits for readyCh to be closed before starting (Databricks must be ready).
// It retries on transient errors with configurable/unlimited retries.
func (sc *SubscriberClient) Run(ctx context.Context, eventCh chan<- *CDCEvent, readyCh <-chan struct{}, replayType, replayID string) error {
	// Wait for Databricks to be ready
	sc.logger.Info("Waiting for Databricks stream initialization...")
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-readyCh:
		sc.logger.Info("Databricks ready, starting Salesforce subscription")
	}

	currentReplayType := replayType
	currentReplayID := replayID

	for attempt := 0; sc.cfg.MaxRetries == 0 || attempt < sc.cfg.MaxRetries; attempt++ {
		sc.logger.Info("Starting subscription",
			"topic", sc.cfg.Topic(),
			"attempt", attempt+1,
			"replay_type", currentReplayType,
			"replay_id", currentReplayID,
		)

		lastReplayID, err := sc.subscribe(ctx, eventCh, currentReplayType, currentReplayID)
		if err == nil || ctx.Err() != nil {
			return ctx.Err()
		}

		// Determine retry strategy
		newReplayType, newReplayID, shouldRetry := sc.determineRetryStrategy(err, lastReplayID, currentReplayType, currentReplayID)
		if !shouldRetry {
			return fmt.Errorf("non-retryable error: %w", err)
		}

		currentReplayType = newReplayType
		currentReplayID = newReplayID

		// Handle auth refresh on UNAUTHENTICATED
		if s, ok := status.FromError(err); ok && s.Code() == codes.Unauthenticated {
			if err := sc.refreshAuth(ctx); err != nil {
				sc.logger.Error("Failed to refresh authentication", "error", err)
			}
		}

		// Exponential backoff
		sc.logger.Info("Retrying subscription", "delay_attempt", attempt)
		if err := backoff.Sleep(ctx, attempt, 1*time.Second, 60*time.Second); err != nil {
			return err
		}
	}

	return fmt.Errorf("exhausted %d subscription retries", sc.cfg.MaxRetries)
}

// subscribe opens a bidi stream and processes responses until error or context cancellation.
func (sc *SubscriberClient) subscribe(ctx context.Context, eventCh chan<- *CDCEvent, replayType, replayID string) (lastReplayID []byte, err error) {
	md := sc.creds.GRPCMetadata()
	streamCtx := metadata.NewOutgoingContext(ctx, md)

	stream, err := sc.stub.Subscribe(streamCtx)
	if err != nil {
		return nil, fmt.Errorf("opening Subscribe stream: %w", err)
	}

	// Flow control channel: response handler signals when ready for next FetchRequest
	flowCh := make(chan struct{}, 1)

	// Error channels for the two goroutines
	sendErrCh := make(chan error, 1)
	recvErrCh := make(chan error, 1)

	// Goroutine: send FetchRequests with flow control
	go func() {
		defer close(sendErrCh)
		err := sc.sendFetchRequests(ctx, stream, flowCh, replayType, replayID)
		sendErrCh <- err
	}()

	// Process responses on the current goroutine
	go func() {
		defer close(recvErrCh)
		var recvErr error
		for {
			resp, err := stream.Recv()
			if err != nil {
				logGRPCError(sc.logger, err, "receiving from Subscribe stream")
				recvErr = err
				break
			}

			// Track replay ID
			if len(resp.GetLatestReplayId()) > 0 {
				lastReplayID = resp.GetLatestReplayId()
			}

			// Process events
			if len(resp.GetEvents()) > 0 {
				for _, consumerEvent := range resp.GetEvents() {
					if len(consumerEvent.GetReplayId()) > 0 {
						lastReplayID = consumerEvent.GetReplayId()
					}

					event := consumerEvent.GetEvent()
					if event == nil {
						continue
					}

					// Deduplication
					eventID := event.GetId()
					if eventID != "" && sc.dedup.IsDuplicate(eventID) {
						sc.logger.Debug("Skipping duplicate event", "event_id", eventID)
						continue
					}
					if eventID != "" {
						sc.dedup.MarkProcessed(eventID)
					}

					// Decode Avro payload
					cdcEvent, err := sc.decodeEvent(ctx, event, consumerEvent.GetReplayId())
					if err != nil {
						sc.logger.Error("Failed to decode event", "error", err, "event_id", eventID)
						continue
					}

					// Send to event channel (blocks if full = backpressure)
					select {
					case <-ctx.Done():
						recvErr = ctx.Err()
						break
					case eventCh <- cdcEvent:
						sc.logger.Info("Received event",
							"entity", cdcEvent.EntityName,
							"change_type", cdcEvent.ChangeType,
							"record_id", firstOrDefault(cdcEvent.RecordIDs, "unknown"),
						)
					}
				}
			} else {
				sc.logger.Debug("Keepalive received")
			}

			// Signal flow control: ready for next FetchRequest
			select {
			case flowCh <- struct{}{}:
			default:
			}
		}
		recvErrCh <- recvErr
	}()

	// Wait for either goroutine to finish
	select {
	case err := <-recvErrCh:
		return lastReplayID, err
	case err := <-sendErrCh:
		if err != nil {
			return lastReplayID, err
		}
		// Sender exited cleanly, wait for receiver
		return lastReplayID, <-recvErrCh
	case <-ctx.Done():
		return lastReplayID, ctx.Err()
	}
}

// sendFetchRequests manages the request side of the bidi stream.
// Implements Salesforce's 60-second FetchRequest compliance requirement.
func (sc *SubscriberClient) sendFetchRequests(ctx context.Context, stream pb.PubSub_SubscribeClient, flowCh <-chan struct{}, replayType, replayID string) error {
	// Send initial FetchRequest
	req := sc.makeFetchRequest(sc.cfg.Topic(), replayType, replayID, int32(sc.cfg.BatchSize))
	if err := stream.Send(req); err != nil {
		return fmt.Errorf("sending initial FetchRequest: %w", err)
	}

	timeout := time.Duration(sc.cfg.TimeoutSeconds * float64(time.Second))

	for {
		select {
		case <-ctx.Done():
			stream.CloseSend()
			return ctx.Err()
		case <-flowCh:
			// Batch processed, send next FetchRequest
			req := sc.makeFetchRequest(sc.cfg.Topic(), replayType, replayID, int32(sc.cfg.BatchSize))
			if err := stream.Send(req); err != nil {
				return fmt.Errorf("sending FetchRequest: %w", err)
			}
		case <-time.After(timeout):
			// Salesforce 60-second compliance: send FetchRequest even without events
			sc.logger.Debug("Sending compliance FetchRequest", "timeout_seconds", sc.cfg.TimeoutSeconds)
			req := sc.makeFetchRequest(sc.cfg.Topic(), replayType, replayID, int32(sc.cfg.BatchSize))
			if err := stream.Send(req); err != nil {
				return fmt.Errorf("sending compliance FetchRequest: %w", err)
			}
		}
	}
}

func (sc *SubscriberClient) makeFetchRequest(topic, replayType, replayID string, numRequested int32) *pb.FetchRequest {
	req := &pb.FetchRequest{
		TopicName:    topic,
		NumRequested: numRequested,
	}

	switch replayType {
	case "EARLIEST":
		preset := pb.ReplayPreset_EARLIEST
		req.ReplayPreset = preset
	case "CUSTOM":
		preset := pb.ReplayPreset_CUSTOM
		req.ReplayPreset = preset
		if replayID != "" {
			if replayBytes, err := hex.DecodeString(replayID); err == nil {
				req.ReplayId = replayBytes
			} else {
				req.ReplayId = []byte(replayID)
			}
		}
	default: // LATEST
		preset := pb.ReplayPreset_LATEST
		req.ReplayPreset = preset
	}

	return req
}

// decodeEvent decodes an Avro-encoded ProducerEvent into a CDCEvent.
func (sc *SubscriberClient) decodeEvent(ctx context.Context, event *pb.ProducerEvent, replayID []byte) (*CDCEvent, error) {
	schemaID := event.GetSchemaId()
	payload := event.GetPayload()

	// Get or fetch the Avro schema
	md := sc.creds.GRPCMetadata()
	codec, schemaJSON, err := sc.schemas.GetOrFetch(ctx, schemaID, sc.stub, md)
	if err != nil {
		return nil, fmt.Errorf("getting schema %s: %w", schemaID, err)
	}

	// Decode the Avro payload
	decoded, err := DecodeAvro(codec, payload)
	if err != nil {
		return nil, fmt.Errorf("decoding Avro payload: %w", err)
	}

	cdcEvent := &CDCEvent{
		EventID:       event.GetId(),
		SchemaID:      schemaID,
		ReplayID:      hex.EncodeToString(replayID),
		PayloadBinary: payload,
		SchemaJSON:    schemaJSON,
	}

	// Extract ChangeEventHeader
	if header, ok := decoded["ChangeEventHeader"]; ok {
		if headerMap, ok := unwrapUnion(header).(map[string]interface{}); ok {
			cdcEvent.ChangeType = getStringField(headerMap, "changeType")
			cdcEvent.EntityName = getStringField(headerMap, "entityName")
			cdcEvent.ChangeOrigin = getStringField(headerMap, "changeOrigin")
			cdcEvent.RecordIDs = getStringSliceField(headerMap, "recordIds")

			// Process bitmap fields
			if changedFields, ok := headerMap["changedFields"]; ok {
				if bitmaps := toInterfaceSlice(changedFields); len(bitmaps) > 0 {
					if names, err := ProcessBitmap(schemaJSON, bitmaps); err == nil {
						cdcEvent.ChangedFields = names
					} else {
						sc.logger.Warn("Failed to process changedFields bitmap", "error", err)
					}
				}
			}
			if nulledFields, ok := headerMap["nulledFields"]; ok {
				if bitmaps := toInterfaceSlice(nulledFields); len(bitmaps) > 0 {
					if names, err := ProcessBitmap(schemaJSON, bitmaps); err == nil {
						cdcEvent.NulledFields = names
					} else {
						sc.logger.Warn("Failed to process nulledFields bitmap", "error", err)
					}
				}
			}
			if diffFields, ok := headerMap["diffFields"]; ok {
				if bitmaps := toInterfaceSlice(diffFields); len(bitmaps) > 0 {
					if names, err := ProcessBitmap(schemaJSON, bitmaps); err == nil {
						cdcEvent.DiffFields = names
					} else {
						sc.logger.Warn("Failed to process diffFields bitmap", "error", err)
					}
				}
			}
		}
	}

	// Serialize the full decoded record as JSON
	recordJSON, err := json.Marshal(decoded)
	if err != nil {
		sc.logger.Warn("Failed to marshal record data to JSON", "error", err)
		cdcEvent.RecordDataJSON = "{}"
	} else {
		cdcEvent.RecordDataJSON = string(recordJSON)
	}

	return cdcEvent, nil
}

// determineRetryStrategy decides how to retry based on the error.
func (sc *SubscriberClient) determineRetryStrategy(err error, lastReplayID []byte, origType, origID string) (replayType, replayID string, shouldRetry bool) {
	s, ok := status.FromError(err)
	if !ok {
		return origType, origID, false
	}

	code := s.Code()

	switch {
	case code == codes.InvalidArgument:
		// Stale or invalid replay ID — fall back to LATEST
		sc.logger.Warn("InvalidArgument error, falling back to LATEST",
			"message", s.Message(),
			"original_replay_type", origType,
		)
		return "LATEST", "", true

	case code == codes.Unauthenticated:
		return origType, origID, true

	case code == codes.Unavailable || code == codes.Internal || code == codes.Aborted:
		if len(lastReplayID) > 0 {
			return "CUSTOM", hex.EncodeToString(lastReplayID), true
		}
		return origType, origID, true

	case code == codes.ResourceExhausted:
		if len(lastReplayID) > 0 {
			return "CUSTOM", hex.EncodeToString(lastReplayID), true
		}
		return "LATEST", "", true

	case code == codes.DeadlineExceeded:
		if len(lastReplayID) > 0 {
			return "CUSTOM", hex.EncodeToString(lastReplayID), true
		}
		return origType, origID, true

	case isRetryable(code):
		return origType, origID, true

	default:
		return "", "", false
	}
}

func isRetryable(code codes.Code) bool {
	switch code {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Internal,
		codes.ResourceExhausted, codes.Aborted, codes.Unauthenticated:
		return true
	default:
		return false
	}
}

func (sc *SubscriberClient) refreshAuth(ctx context.Context) error {
	if sc.reauth == nil {
		return fmt.Errorf("no re-authentication function configured")
	}
	newCreds, err := sc.reauth(ctx)
	if err != nil {
		return err
	}
	sc.creds = newCreds
	sc.logger.Info("Authentication refreshed successfully")
	return nil
}

// recreateConnection closes the existing gRPC connection and creates a new one.
func (sc *SubscriberClient) RecreateConnection(ctx context.Context) error {
	sc.logger.Info("Recreating gRPC connection")
	if sc.conn != nil {
		sc.conn.Close()
	}

	conn, err := dialGRPC(sc.cfg.GRPCAddress())
	if err != nil {
		return fmt.Errorf("re-dialing gRPC: %w", err)
	}

	sc.conn = conn
	sc.stub = pb.NewPubSubClient(conn)

	if err := sc.refreshAuth(ctx); err != nil {
		return fmt.Errorf("re-authenticating after reconnect: %w", err)
	}

	sc.logger.Info("gRPC connection recreated successfully")
	return nil
}

// Close closes the gRPC connection.
func (sc *SubscriberClient) Close() error {
	if sc.conn != nil {
		return sc.conn.Close()
	}
	return nil
}

// logGRPCError logs a gRPC error with extracted details.
func logGRPCError(logger *slog.Logger, err error, context string) {
	s, ok := status.FromError(err)
	if !ok {
		logger.Error("Non-gRPC error", "context", context, "error", err)
		return
	}
	logger.Error("gRPC error",
		"context", context,
		"code", s.Code().String(),
		"message", s.Message(),
	)
}

// Helper functions for extracting fields from decoded Avro maps.

func unwrapUnion(v interface{}) interface{} {
	if m, ok := v.(map[string]interface{}); ok && len(m) == 1 {
		for _, val := range m {
			return val
		}
	}
	return v
}

func getStringField(m map[string]interface{}, key string) string {
	v, ok := m[key]
	if !ok {
		return ""
	}
	v = unwrapUnion(v)
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

func getStringSliceField(m map[string]interface{}, key string) []string {
	v, ok := m[key]
	if !ok {
		return nil
	}
	v = unwrapUnion(v)
	arr, ok := v.([]interface{})
	if !ok {
		return nil
	}
	result := make([]string, 0, len(arr))
	for _, item := range arr {
		item = unwrapUnion(item)
		if s, ok := item.(string); ok {
			result = append(result, s)
		} else {
			result = append(result, fmt.Sprintf("%v", item))
		}
	}
	return result
}

func toInterfaceSlice(v interface{}) []interface{} {
	v = unwrapUnion(v)
	if arr, ok := v.([]interface{}); ok {
		return arr
	}
	return nil
}

func firstOrDefault(slice []string, defaultVal string) string {
	if len(slice) > 0 {
		return slice[0]
	}
	return defaultVal
}
