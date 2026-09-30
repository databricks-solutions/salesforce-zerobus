// Package fakepubsub is an in-process Salesforce Pub/Sub API server for tests
// and load generation. It keeps an event log per (org, topic), honours replay
// presets and num_requested flow control, sends keepalives, authenticates
// against a token checker, and supports error injection.
package fakepubsub

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/proto/pubsubpb"
)

// TokenChecker returns the org ID for a valid access token.
type TokenChecker func(token string) (orgID string, ok bool)

// Server is a fake Pub/Sub API.
type Server struct {
	pubsubpb.UnimplementedPubSubServer

	Addr string
	// Keepalive is how often an idle stream gets an empty response.
	Keepalive time.Duration

	grpc *grpc.Server
	lis  net.Listener
	auth TokenChecker

	mu       sync.Mutex
	logs     map[string]*topicLog // org|topic
	schemas  map[string]string
	failNext []error
	streams  map[*activeStream]struct{}

	subscribes   atomic.Int64
	schemaCalls  atomic.Int64
	fetchedTotal atomic.Int64
}

type activeStream struct {
	org, topic string
	kill       chan error
}

type topicLog struct {
	mu        sync.Mutex
	events    []*pubsubpb.ConsumerEvent
	retention int // index of the oldest retained event
	changed   chan struct{}
}

// Start listens on 127.0.0.1 (random port) and serves.
func Start(auth TokenChecker) (*Server, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	s := &Server{
		Addr: lis.Addr().String(), Keepalive: 270 * time.Second,
		lis: lis, auth: auth, grpc: grpc.NewServer(),
		logs: map[string]*topicLog{}, schemas: map[string]string{}, streams: map[*activeStream]struct{}{},
	}
	pubsubpb.RegisterPubSubServer(s.grpc, s)
	go s.grpc.Serve(lis)
	return s, nil
}

// Stop stops the server.
func (s *Server) Stop() { s.grpc.Stop() }

// ReplayID encodes a log position.
func ReplayID(pos int) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(pos))
	return b
}

// Position decodes a replay ID produced by ReplayID.
func Position(id []byte) (int, bool) {
	if len(id) != 8 {
		return 0, false
	}
	return int(binary.BigEndian.Uint64(id)), true
}

func (s *Server) log(org, topic string) *topicLog {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := org + "|" + topic
	l, ok := s.logs[k]
	if !ok {
		l = &topicLog{changed: make(chan struct{})}
		s.logs[k] = l
	}
	return l
}

// AddSchema registers a schema.
func (s *Server) AddSchema(id, schemaJSON string) {
	s.mu.Lock()
	s.schemas[id] = schemaJSON
	s.mu.Unlock()
}

// Emit appends an event (as if a record changed in the org) and returns its replay ID.
func (s *Server) Emit(org, topic, schemaID string, payload []byte) []byte {
	l := s.log(org, topic)
	l.mu.Lock()
	defer l.mu.Unlock()
	pos := len(l.events) + 1 // replay IDs start at 1
	id := ReplayID(pos)
	l.events = append(l.events, &pubsubpb.ConsumerEvent{
		Event:    &pubsubpb.ProducerEvent{Id: uuid.NewString(), SchemaId: schemaID, Payload: payload},
		ReplayId: id,
	})
	close(l.changed)
	l.changed = make(chan struct{})
	return id
}

// Expire drops events before position pos from retention.
func (s *Server) Expire(org, topic string, pos int) {
	l := s.log(org, topic)
	l.mu.Lock()
	l.retention = min(pos-1, len(l.events))
	l.mu.Unlock()
}

// Count returns the number of events published to (org, topic).
func (s *Server) Count(org, topic string) int {
	l := s.log(org, topic)
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.events)
}

// FailNextSubscribes makes the next Subscribe calls fail with errs, in order.
func (s *Server) FailNextSubscribes(errs ...error) {
	s.mu.Lock()
	s.failNext = append(s.failNext, errs...)
	s.mu.Unlock()
}

// KillStreams ends active streams for org (all orgs if empty) with err.
func (s *Server) KillStreams(org string, err error) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for st := range s.streams {
		if org == "" || st.org == org {
			select {
			case st.kill <- err:
				n++
			default:
			}
		}
	}
	return n
}

// ActiveStreams returns the number of open Subscribe streams.
func (s *Server) ActiveStreams() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.streams)
}

// Subscribes returns the number of Subscribe calls.
func (s *Server) Subscribes() int64 { return s.subscribes.Load() }

// Delivered returns the number of events sent to subscribers.
func (s *Server) Delivered() int64 { return s.fetchedTotal.Load() }

func (s *Server) authenticate(ctx context.Context) (string, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	get := func(k string) string {
		if v := md.Get(k); len(v) > 0 {
			return v[0]
		}
		return ""
	}
	org, ok := s.auth(get("accesstoken"))
	if !ok || get("tenantid") != org || get("instanceurl") == "" {
		return "", status.Error(codes.Unauthenticated, "invalid access token")
	}
	return org, nil
}

func (s *Server) GetSchema(ctx context.Context, req *pubsubpb.SchemaRequest) (*pubsubpb.SchemaInfo, error) {
	if _, err := s.authenticate(ctx); err != nil {
		return nil, err
	}
	s.schemaCalls.Add(1)
	s.mu.Lock()
	js, ok := s.schemas[req.GetSchemaId()]
	s.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "schema %s not found", req.GetSchemaId())
	}
	return &pubsubpb.SchemaInfo{SchemaJson: js, SchemaId: req.GetSchemaId()}, nil
}

func (s *Server) GetTopic(ctx context.Context, req *pubsubpb.TopicRequest) (*pubsubpb.TopicInfo, error) {
	if _, err := s.authenticate(ctx); err != nil {
		return nil, err
	}
	return &pubsubpb.TopicInfo{TopicName: req.GetTopicName(), CanSubscribe: true}, nil
}

func (s *Server) Subscribe(stream pubsubpb.PubSub_SubscribeServer) error {
	s.subscribes.Add(1)
	org, err := s.authenticate(stream.Context())
	if err != nil {
		return err
	}
	s.mu.Lock()
	if len(s.failNext) > 0 {
		err := s.failNext[0]
		s.failNext = s.failNext[1:]
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()

	first, err := stream.Recv()
	if err != nil {
		return err
	}
	topic := first.GetTopicName()
	l := s.log(org, topic)

	l.mu.Lock()
	var pos int // index of the next event to deliver
	switch first.GetReplayPreset() {
	case pubsubpb.ReplayPreset_EARLIEST:
		pos = l.retention
	case pubsubpb.ReplayPreset_CUSTOM:
		p, ok := Position(first.GetReplayId())
		if !ok || p > len(l.events) || p < l.retention {
			l.mu.Unlock()
			return status.Error(codes.InvalidArgument, "The Replay ID validation failed.")
		}
		pos = p // deliver events after the given replay ID
	default:
		pos = len(l.events)
	}
	l.mu.Unlock()

	var pending atomic.Int64
	pending.Add(int64(first.GetNumRequested()))
	more := make(chan struct{}, 1)
	recvErr := make(chan error, 1)
	go func() {
		for {
			req, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			pending.Add(int64(req.GetNumRequested()))
			select {
			case more <- struct{}{}:
			default:
			}
		}
	}()

	as := &activeStream{org: org, topic: topic, kill: make(chan error, 1)}
	s.mu.Lock()
	s.streams[as] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.streams, as)
		s.mu.Unlock()
	}()

	keepalive := time.NewTimer(s.Keepalive)
	defer keepalive.Stop()
	for {
		l.mu.Lock()
		available := len(l.events) - pos
		changed := l.changed
		var batch []*pubsubpb.ConsumerEvent
		if n := min(int64(available), pending.Load(), 100); n > 0 {
			batch = append(batch, l.events[pos:pos+int(n)]...)
			pos += int(n)
		}
		latest := ReplayID(len(l.events))
		l.mu.Unlock()

		if len(batch) > 0 {
			left := pending.Add(-int64(len(batch)))
			s.fetchedTotal.Add(int64(len(batch)))
			if err := stream.Send(&pubsubpb.FetchResponse{Events: batch, LatestReplayId: batch[len(batch)-1].ReplayId,
				RpcId: uuid.NewString(), PendingNumRequested: int32(left)}); err != nil {
				return err
			}
			keepalive.Reset(s.Keepalive)
			continue
		}
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case err := <-recvErr:
			return err
		case err := <-as.kill:
			return err
		case <-more:
		case <-changed:
		case <-keepalive.C:
			if err := stream.Send(&pubsubpb.FetchResponse{LatestReplayId: latest, RpcId: uuid.NewString(),
				PendingNumRequested: int32(pending.Load())}); err != nil {
				return err
			}
			keepalive.Reset(s.Keepalive)
		}
	}
}

// String describes the server state (for load reports).
func (s *Server) String() string {
	return fmt.Sprintf("subscribes=%d active=%d delivered=%d schema_calls=%d",
		s.Subscribes(), s.ActiveStreams(), s.Delivered(), s.schemaCalls.Load())
}
