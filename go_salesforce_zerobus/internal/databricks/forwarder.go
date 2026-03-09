package databricks

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"

	"github.com/databricks-solutions/go-salesforce-zerobus/internal/backoff"
	"github.com/databricks-solutions/go-salesforce-zerobus/internal/config"
	"github.com/databricks-solutions/go-salesforce-zerobus/internal/pubsub"
	pb "github.com/databricks-solutions/go-salesforce-zerobus/proto/gen"

	zerobus "github.com/databricks/zerobus-sdk/go"
)

// StreamHealth reports the current state of the Zerobus stream.
type StreamHealth struct {
	Status  string
	Healthy bool
}

// Forwarder manages the Zerobus SDK stream and ingests CDC events.
type Forwarder struct {
	sdk    *zerobus.ZerobusSdk
	stream *zerobus.ZerobusStream
	cfg    *config.Config
	logger *slog.Logger
	mu     sync.Mutex
}

// NewForwarder creates a new Databricks Zerobus forwarder.
func NewForwarder(cfg *config.Config, logger *slog.Logger) *Forwarder {
	return &Forwarder{
		cfg:    cfg,
		logger: logger,
	}
}

// Run is the main forwarder loop. It initializes the stream, signals readiness,
// then consumes events from eventCh and ingests them via Zerobus.
// Events are never dropped - failures are retried with backoff.
func (f *Forwarder) Run(ctx context.Context, eventCh <-chan *pubsub.CDCEvent, readyCh chan<- struct{}) error {
	if err := f.initializeStream(); err != nil {
		return fmt.Errorf("initializing Zerobus stream: %w", err)
	}

	// Signal that Databricks is ready
	close(readyCh)
	f.logger.Info("Zerobus stream initialized, ready for events")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-eventCh:
			if !ok {
				return nil // channel closed
			}

			// Retry loop: never drop events
			for attempt := 0; ; attempt++ {
				err := f.forwardEvent(event)
				if err == nil {
					break
				}

				f.logger.Warn("Event forwarding failed, retrying",
					"attempt", attempt+1,
					"error", err,
					"entity", event.EntityName,
					"event_id", event.EventID,
				)

				if err := backoff.Sleep(ctx, attempt, 1*time.Second, 60*time.Second); err != nil {
					return err // context cancelled
				}
			}
		}
	}
}

// buildStreamOpts creates StreamConfigurationOptions from config.
func (f *Forwarder) buildStreamOpts() *zerobus.StreamConfigurationOptions {
	opts := zerobus.DefaultStreamConfigurationOptions()
	opts.RecordType = zerobus.RecordTypeProto
	opts.Recovery = true
	opts.RecoveryRetries = uint32(f.cfg.ZerobusRecoveryRetries)
	opts.RecoveryTimeoutMs = uint64(f.cfg.ZerobusRecoveryTimeoutMs)
	opts.RecoveryBackoffMs = uint64(f.cfg.ZerobusRecoveryBackoffMs)
	opts.MaxInflightRequests = uint64(f.cfg.ZerobusMaxInflight)
	opts.ServerLackOfAckTimeoutMs = uint64(f.cfg.ZerobusServerAckTimeoutMs)
	opts.FlushTimeoutMs = uint64(f.cfg.ZerobusFlushTimeoutMs)
	return opts
}

func (f *Forwarder) initializeStream() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.initializeStreamLocked()
}

// initializeStreamLocked creates the SDK and stream. Must be called with f.mu held.
func (f *Forwarder) initializeStreamLocked() error {
	sdk, err := zerobus.NewZerobusSdk(f.cfg.DBIngestEndpoint, f.cfg.DBWorkspaceURL)
	if err != nil {
		return fmt.Errorf("creating Zerobus SDK: %w", err)
	}
	f.sdk = sdk

	descriptorBytes, err := getSalesforceEventDescriptor()
	if err != nil {
		return fmt.Errorf("getting proto descriptor: %w", err)
	}

	tableProps := zerobus.TableProperties{
		TableName:       f.cfg.DBTableName,
		DescriptorProto: descriptorBytes,
	}

	stream, err := sdk.CreateStream(tableProps, f.cfg.DBClientID, f.cfg.DBClientSecret, f.buildStreamOpts())
	if err != nil {
		return fmt.Errorf("creating Zerobus stream: %w", err)
	}
	f.stream = stream

	f.logger.Info("Zerobus stream created", "table", f.cfg.DBTableName)
	return nil
}

// forwardEvent converts a CDCEvent to protobuf and ingests it via Zerobus.
func (f *Forwarder) forwardEvent(event *pubsub.CDCEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.stream == nil {
		if err := f.initializeStreamLocked(); err != nil {
			return err
		}
	}

	now := time.Now().UnixMilli()

	pbEvent := &pb.SalesforceEvent{
		EventId:            proto.String(event.EventID),
		SchemaId:           proto.String(event.SchemaID),
		ReplayId:           proto.String(event.ReplayID),
		Timestamp:          proto.Int64(now),
		ChangeType:         proto.String(event.ChangeType),
		EntityName:         proto.String(event.EntityName),
		ChangeOrigin:       proto.String(event.ChangeOrigin),
		RecordIds:          event.RecordIDs,
		ChangedFields:      event.ChangedFields,
		NulledFields:       event.NulledFields,
		DiffFields:         event.DiffFields,
		RecordDataJson:     proto.String(event.RecordDataJSON),
		PayloadBinary:      event.PayloadBinary,
		SchemaJson:         proto.String(event.SchemaJSON),
		OrgId:              proto.String(event.OrgID),
		ProcessedTimestamp: proto.Int64(now),
	}

	data, err := proto.Marshal(pbEvent)
	if err != nil {
		return fmt.Errorf("marshaling protobuf: %w", err)
	}

	if _, err := f.stream.IngestRecordOffset(data); err != nil {
		f.logger.Warn("Ingest failed, recreating stream", "error", err)
		if recreateErr := f.recreateStream(); recreateErr != nil {
			return fmt.Errorf("stream recreation failed: %w (original: %v)", recreateErr, err)
		}
		// Retry with new stream
		if _, err := f.stream.IngestRecordOffset(data); err != nil {
			f.stream = nil
			return fmt.Errorf("ingest failed after recreation: %w", err)
		}
	}

	recordID := "unknown"
	if len(event.RecordIDs) > 0 {
		recordID = event.RecordIDs[0]
	}
	f.logger.Info("Ingested to Databricks",
		"table", f.cfg.DBTableName,
		"entity", event.EntityName,
		"change_type", event.ChangeType,
		"record_id", recordID,
	)
	return nil
}

// recreateStream recreates the Zerobus stream with exponential backoff.
func (f *Forwarder) recreateStream() error {
	for attempt := 0; attempt < 5; attempt++ {
		f.logger.Info("Attempting stream recreation", "attempt", attempt+1)

		if f.stream != nil {
			f.stream.Close()
		}

		descriptorBytes, err := getSalesforceEventDescriptor()
		if err != nil {
			return fmt.Errorf("getting proto descriptor: %w", err)
		}

		tableProps := zerobus.TableProperties{
			TableName:       f.cfg.DBTableName,
			DescriptorProto: descriptorBytes,
		}

		stream, err := f.sdk.CreateStream(tableProps, f.cfg.DBClientID, f.cfg.DBClientSecret, f.buildStreamOpts())
		if err == nil {
			f.stream = stream
			f.logger.Info("Stream recreated successfully")
			return nil
		}

		f.logger.Warn("Stream recreation attempt failed", "attempt", attempt+1, "error", err)
		// Use a simple sleep here since we're already holding the mutex
		time.Sleep(time.Duration(1<<uint(attempt)) * time.Second)
	}
	f.stream = nil
	return fmt.Errorf("failed to recreate stream after 5 attempts")
}

// GetHealth returns the current stream health status.
func (f *Forwarder) GetHealth() StreamHealth {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.stream == nil {
		return StreamHealth{Status: "no_stream", Healthy: false}
	}
	return StreamHealth{Status: "active", Healthy: true}
}

// Close flushes pending records and closes the stream.
func (f *Forwarder) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.stream != nil {
		if err := f.stream.Close(); err != nil {
			f.logger.Warn("Error closing stream", "error", err)
		}
		f.stream = nil
	}
	if f.sdk != nil {
		f.sdk.Free()
		f.sdk = nil
	}
	f.logger.Info("Zerobus forwarder closed")
	return nil
}

// getSalesforceEventDescriptor extracts the DescriptorProto bytes for the
// SalesforceEvent message, which Zerobus needs for proto-mode ingestion.
func getSalesforceEventDescriptor() ([]byte, error) {
	// Use protodesc to correctly convert the reflection descriptor
	fd := (&pb.SalesforceEvent{}).ProtoReflect().Descriptor().ParentFile()
	fdp := protodesc.ToFileDescriptorProto(fd)

	// Extract the message-level DescriptorProto for SalesforceEvent
	for _, msg := range fdp.MessageType {
		if msg.GetName() == "SalesforceEvent" {
			return proto.Marshal(msg)
		}
	}
	return nil, fmt.Errorf("SalesforceEvent message not found in file descriptor")
}
