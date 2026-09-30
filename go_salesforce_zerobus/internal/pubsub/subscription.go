package pubsub

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
	"google.golang.org/grpc/metadata"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/checkpoint"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/event"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/obs"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/sfauth"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/sink"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/tenant"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/proto/pubsubpb"
)

// keepaliveAfter is how long the fetch loop waits with nothing requested
// before sending a FetchRequest anyway (Salesforce closes streams that go 60s
// without one when nothing is pending).
const keepaliveAfter = 50 * time.Second

// Shared holds dependencies shared by every subscription on the replica.
type Shared struct {
	Conns            *ConnPool
	Schemas          *SchemaCache
	SubscribeLimiter *rate.Limiter
	SchemaLimiter    *rate.Limiter
	MaxRowBytes      int
	Logger           *slog.Logger
}

// Start says where a subscription begins.
type Start struct {
	Preset   tenant.Preset // used when ReplayID is empty
	ReplayID []byte
}

func (s Start) String() string {
	if len(s.ReplayID) > 0 {
		return fmt.Sprintf("CUSTOM(%x)", s.ReplayID)
	}
	return string(s.Preset)
}

// Stats are live counters for one subscription (safe for concurrent reads).
type Stats struct {
	Received    atomic.Int64
	Submitted   atomic.Int64
	Duplicates  atomic.Int64
	DecodeErrs  atomic.Int64
	LastEventAt atomic.Int64 // unix nanos
	Pending     atomic.Int64 // events requested from Salesforce, not yet delivered
}

// Subscription streams one (tenant, topic) from Salesforce into the sink.
type Subscription struct {
	Spec          tenant.SubscriptionSpec
	ExpectedOrgID string
	Tokens        *sfauth.TokenSource
	Writer        sink.Writer
	Watermark     *checkpoint.Watermark
	Dedup         *DedupCache
	Stats         *Stats
	Shared        *Shared
	Logger        *slog.Logger
}

// Run subscribes from start under generation gen until ctx is done or an
// error occurs. The returned error is always an *Error.
func (s *Subscription) Run(ctx context.Context, gen uint64, start Start) error {
	creds, err := s.Tokens.Get(ctx)
	if err != nil {
		return classify(err)
	}
	if s.ExpectedOrgID != "" && !sameOrg(creds.OrgID, s.ExpectedOrgID) {
		return classify(fmt.Errorf("%w: got %s, want %s", errOrgMismatch, creds.OrgID, s.ExpectedOrgID))
	}
	s.Watermark.SetOrg(creds.OrgID)

	if l := s.Shared.SubscribeLimiter; l != nil {
		began := time.Now()
		if err := l.Wait(ctx); err != nil {
			return classify(err)
		}
		obs.RateLimitWait.WithLabelValues("subscribe").Observe(time.Since(began).Seconds())
	}
	client, release, err := s.Shared.Conns.Acquire()
	if err != nil {
		return classify(err)
	}
	defer release()

	streamCtx, cancel := context.WithCancel(metadata.NewOutgoingContext(ctx, creds.Metadata()))
	defer cancel()
	began := time.Now()
	stream, err := client.Subscribe(streamCtx)
	if err != nil {
		return s.rpcError("subscribe", creds, err)
	}
	obs.StreamOpenSeconds.WithLabelValues("salesforce").Observe(time.Since(began).Seconds())
	s.Stats.Pending.Store(0)

	errc := make(chan error, 2)
	processed := make(chan struct{}, 1)
	go func() { errc <- s.fetchLoop(streamCtx, stream, start, processed) }()
	go func() { errc <- s.recvLoop(streamCtx, stream, client, creds, gen, processed) }()

	var runErr error
	running := 2
	select {
	case runErr = <-errc:
		running--
	case err := <-s.Watermark.ResetCh():
		runErr = &Error{Class: ClassSinkReset, Err: fmt.Errorf("%w: %v", errSinkReset, err)}
	case <-ctx.Done():
		runErr = ctx.Err()
	}
	cancel()
	for ; running > 0; running-- { // both exit once streamCtx is cancelled
		<-errc
	}
	if ctx.Err() != nil {
		return &Error{Class: ClassCanceled, Err: ctx.Err()}
	}
	return s.rpcError("subscribe", creds, runErr)
}

func (s *Subscription) rpcError(op string, creds *sfauth.Credentials, err error) error {
	e := classify(err).(*Error)
	switch e.Class {
	case ClassAuth:
		s.Tokens.Invalidate(creds)
	case ClassCanceled, ClassSinkReset, ClassSinkUnavailable:
		return e
	}
	obs.SFRPCErrors.WithLabelValues(op, e.Class.String()).Inc()
	return e
}

// fetchLoop sends FetchRequests. Credit is bounded by events not yet acked
// by Zerobus, so a slow sink slows Salesforce down instead of buffering.
func (s *Subscription) fetchLoop(ctx context.Context, stream pubsubpb.PubSub_SubscribeClient, start Start, processed <-chan struct{}) error {
	defer stream.CloseSend()
	first := true
	var lastSend time.Time
	send := func(n int) error {
		req := &pubsubpb.FetchRequest{TopicName: s.Spec.Key.Topic, NumRequested: int32(n)}
		if first {
			req.ReplayPreset = pubsubpb.ReplayPreset_LATEST
			switch {
			case len(start.ReplayID) > 0:
				req.ReplayPreset, req.ReplayId = pubsubpb.ReplayPreset_CUSTOM, start.ReplayID
			case start.Preset == tenant.Earliest:
				req.ReplayPreset = pubsubpb.ReplayPreset_EARLIEST
			}
		}
		if err := stream.Send(req); err != nil {
			return err
		}
		first = false
		lastSend = time.Now()
		s.Stats.Pending.Add(int64(n))
		return nil
	}

	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for {
		credit := int64(s.Spec.MaxUnacked) - s.Watermark.Unacked() - s.Stats.Pending.Load()
		pending := s.Stats.Pending.Load()
		switch {
		case pending <= 0 && credit > 0:
			if err := send(int(min(credit, int64(s.Spec.BatchSize)))); err != nil {
				return err
			}
		case pending <= 0 && (first || time.Since(lastSend) >= keepaliveAfter):
			// No credit (sink is behind) but the stream needs a request to
			// stay open; overshooting the budget by one is harmless.
			if err := send(1); err != nil {
				return err
			}
		}
		wait := keepaliveAfter - time.Since(lastSend)
		if wait <= 0 {
			wait = time.Second
		}
		timer.Reset(wait)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-processed:
		case <-s.Watermark.Notify():
		case <-timer.C:
		}
	}
}

func (s *Subscription) recvLoop(ctx context.Context, stream pubsubpb.PubSub_SubscribeClient, client pubsubpb.PubSubClient,
	creds *sfauth.Credentials, gen uint64, processed chan<- struct{}) error {
	table := s.Spec.Table
	src := event.Source{OrgID: creds.OrgID, TenantKey: string(s.Spec.Key.Tenant), Topic: s.Spec.Key.Topic}
	fetchSchema := func(ctx context.Context, id string) (string, error) {
		if l := s.Shared.SchemaLimiter; l != nil {
			if err := l.Wait(ctx); err != nil {
				return "", err
			}
		}
		resp, err := client.GetSchema(metadata.NewOutgoingContext(ctx, creds.Metadata()), &pubsubpb.SchemaRequest{SchemaId: id})
		if err != nil {
			return "", err
		}
		return resp.GetSchemaJson(), nil
	}
	notify := func() {
		select {
		case processed <- struct{}{}:
		default:
		}
	}

	for {
		resp, err := stream.Recv()
		if err != nil {
			if trailerMentionsReplay(stream.Trailer()) {
				return &Error{Class: ClassReplayExpired, Err: err}
			}
			return err
		}
		s.Stats.Pending.Store(int64(resp.GetPendingNumRequested()))
		events := resp.GetEvents()
		if len(events) == 0 {
			// Keepalive: carry the stream position forward for quiet orgs.
			s.Watermark.AdvanceIdle(gen, resp.GetLatestReplayId())
			notify()
			continue
		}
		receivedAt := time.Now()
		obs.EventsReceived.WithLabelValues(table).Add(float64(len(events)))
		s.Stats.Received.Add(int64(len(events)))
		s.Stats.LastEventAt.Store(receivedAt.UnixNano())
		for _, ce := range events {
			pe := ce.GetEvent()
			if pe == nil {
				continue
			}
			if s.Dedup.Seen(pe.GetId()) {
				s.Stats.Duplicates.Add(1)
				obs.EventsDuplicate.WithLabelValues(table).Inc()
				continue
			}
			schema, err := s.Shared.Schemas.Get(ctx, creds.OrgID, pe.GetSchemaId(), fetchSchema)
			if err != nil {
				return fmt.Errorf("fetching schema %s: %w", pe.GetSchemaId(), err)
			}
			e := DecodeEvent(schema, pe, ce.GetReplayId())
			if e.DecodeError != "" {
				s.Stats.DecodeErrs.Add(1)
				obs.DecodeErrors.WithLabelValues(table).Inc()
				s.Logger.Warn("Event could not be decoded; writing raw payload", "event_id", e.EventID, "error", e.DecodeError)
			}
			payload, truncated, err := event.Marshal(event.ToProto(e, src, receivedAt), s.Shared.MaxRowBytes)
			if truncated {
				obs.RecordsTruncated.WithLabelValues(table).Inc()
			}
			if err != nil {
				// Cannot happen for real CDC events (the large columns are
				// dropped first); skip rather than wedge the subscription.
				s.Logger.Error("Dropping event that cannot fit in a Zerobus row", "event_id", e.EventID, "error", err)
				continue
			}
			if !s.Watermark.BeginSubmit(gen) {
				return &Error{Class: ClassCanceled, Err: errors.New("subscription restarted")}
			}
			err = s.Writer.Submit(ctx, &sink.Record{Payload: payload, ReplayID: ce.GetReplayId(), Gen: gen, ReceivedAt: receivedAt})
			s.Watermark.EndSubmit(gen, ce.GetReplayId(), err == nil)
			if err != nil {
				return err
			}
			s.Stats.Submitted.Add(1)
			s.Dedup.Mark(pe.GetId())
			s.Logger.Debug("Event submitted", "event_id", e.EventID, "entity", e.EntityName, "change_type", e.ChangeType)
		}
		notify()
	}
}

func trailerMentionsReplay(md metadata.MD) bool {
	for _, v := range md.Get("error-code") {
		if strings.Contains(strings.ToLower(v), "replayid") {
			return true
		}
	}
	return false
}

// sameOrg compares 15- and 18-character org IDs.
func sameOrg(a, b string) bool {
	if len(a) >= 15 && len(b) >= 15 {
		return a[:15] == b[:15]
	}
	return a == b
}
