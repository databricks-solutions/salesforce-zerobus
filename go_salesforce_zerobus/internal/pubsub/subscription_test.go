package pubsub

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/backoff"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/checkpoint"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/sfauth"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/sink"
	zbsink "github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/sink/zerobus"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/tenant"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/testutil/cdc"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/testutil/fakeoauth"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/testutil/fakepubsub"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/proto/eventpb"
)

const (
	orgID = "00DA0000000000AAAA"
	topic = "/data/AccountChangeEvent"
	table = "main.sf.cdc"
)

type env struct {
	t      *testing.T
	oauth  *fakeoauth.Server
	ps     *fakepubsub.Server
	mem    *zbsink.MemoryFactory
	sink   *zbsink.Sink
	tokens *sfauth.TokenSource
	shared *Shared
	schema string
}

func newEnv(t *testing.T, ackLatency time.Duration) *env {
	t.Helper()
	oauth := fakeoauth.New()
	t.Cleanup(oauth.Close)
	oauth.AddClient("cid", "secret", orgID)
	ps, err := fakepubsub.Start(oauth.OrgForToken)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ps.Stop)
	schemaID := cdc.SchemaID(cdc.AccountSchemaJSON)
	ps.AddSchema(schemaID, cdc.AccountSchemaJSON)

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mem := zbsink.NewMemoryFactory(ackLatency)
	s := zbsink.New(mem, zbsink.Options{BatchLinger: time.Millisecond, Logger: logger,
		Reopen: backoff.Policy{Base: 5 * time.Millisecond, Max: 20 * time.Millisecond}})
	t.Cleanup(func() { s.Close(context.Background()) })
	s.Prepare(context.Background(), map[string]int{table: 1})

	auth := sfauth.NewAuthenticator(tenant.Salesforce{InstanceURL: oauth.URL, Auth: tenant.Auth{
		Type: tenant.AuthOAuthClientCredentials, ClientID: "cid", ClientSecretRef: "env://S"}},
		sfauth.NewHTTPClient(), func(context.Context, string) (string, error) { return "secret", nil })
	conns := NewConnPool(ps.Addr, 100, true)
	t.Cleanup(func() { conns.Close() })
	return &env{t: t, oauth: oauth, ps: ps, mem: mem, sink: s,
		tokens: sfauth.NewTokenSource(auth, time.Hour, nil),
		shared: &Shared{Conns: conns, Schemas: NewSchemaCache(0), MaxRowBytes: 8 << 20, Logger: logger},
		schema: schemaID}
}

func (e *env) emit(n int) {
	for i := 0; i < n; i++ {
		payload, err := cdc.EncodeAccount(cdc.AccountEvent{RecordID: "001", ChangeType: "UPDATE", Name: "Acme", CommitNumber: int64(i), ChangedFields: []string{"0x02"}})
		if err != nil {
			e.t.Fatal(err)
		}
		e.ps.Emit(orgID, topic, e.schema, payload)
	}
}

func (e *env) subscription(maxUnacked int) (*Subscription, *checkpoint.Watermark) {
	spec := tenant.SubscriptionSpec{Key: tenant.SubKey{Tenant: "acme", Topic: topic}, Table: table, BatchSize: 50, MaxUnacked: maxUnacked, DedupSize: 100}
	wm := checkpoint.NewRegistry().Register(checkpoint.KeyOf(spec.Key), table, "", nil)
	w, err := e.sink.Open(&sink.SubscriptionRef{Key: spec.Key, Table: table, Acker: wm})
	if err != nil {
		e.t.Fatal(err)
	}
	return &Subscription{Spec: spec, Tokens: e.tokens, Writer: w, Watermark: wm, Dedup: NewDedupCache(100),
		Stats: &Stats{}, Shared: e.shared, Logger: e.shared.Logger}, wm
}

func runAsync(s *Subscription, gen uint64, start Start) (context.CancelFunc, <-chan error) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx, gen, start) }()
	return cancel, done
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestSubscriptionDeliversAndAcks(t *testing.T) {
	e := newEnv(t, 0)
	e.emit(250)
	sub, wm := e.subscription(500)
	cancel, done := runAsync(sub, wm.Restart(), Start{Preset: tenant.Earliest})
	waitFor(t, "all acked", func() bool { return bytes.Equal(wm.Acked(), fakepubsub.ReplayID(250)) })
	e.emit(10) // live events after catch-up
	waitFor(t, "live events", func() bool { return bytes.Equal(wm.Acked(), fakepubsub.ReplayID(260)) })
	cancel()
	if err := <-done; Classify(err) != ClassCanceled {
		t.Fatalf("Run = %v", err)
	}
	rows := e.mem.Rows(table)
	if len(rows) != 260 {
		t.Fatalf("rows = %d", len(rows))
	}
	var row eventpb.SalesforceEvent
	if err := proto.Unmarshal(rows[0], &row); err != nil {
		t.Fatal(err)
	}
	if row.GetOrgId() != orgID || row.GetTopic() != topic || row.GetTenantKey() != "acme" || row.GetChangeType() != "UPDATE" ||
		row.GetEntityName() != "Account" || row.GetReplayId() != "0000000000000001" || len(row.GetChangedFields()) != 1 {
		t.Errorf("row = %v", &row)
	}
	if st := wm.Status(); st.OrgID != orgID || st.TotalAcked != 260 || st.Unacked != 0 {
		t.Errorf("watermark = %+v", st)
	}
}

func TestSubscriptionCreditBoundsUnacked(t *testing.T) {
	e := newEnv(t, 30*time.Millisecond) // slow sink
	e.emit(400)
	sub, wm := e.subscription(60)
	var peak atomic.Int64
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			if u := wm.Unacked() + sub.Stats.Pending.Load(); u > peak.Load() {
				peak.Store(u)
			}
			time.Sleep(time.Millisecond)
		}
	}()
	cancel, done := runAsync(sub, wm.Restart(), Start{Preset: tenant.Earliest})
	waitFor(t, "all acked", func() bool { return bytes.Equal(wm.Acked(), fakepubsub.ReplayID(400)) })
	close(stop)
	cancel()
	<-done
	if p := peak.Load(); p > 61 {
		t.Fatalf("unacked+pending peaked at %d, budget 60", p)
	}
}

func TestSubscriptionResumeCustom(t *testing.T) {
	e := newEnv(t, 0)
	e.emit(30)
	sub, wm := e.subscription(500)
	cancel, done := runAsync(sub, wm.Restart(), Start{ReplayID: fakepubsub.ReplayID(20)})
	waitFor(t, "acked", func() bool { return bytes.Equal(wm.Acked(), fakepubsub.ReplayID(30)) })
	cancel()
	<-done
	if n := len(e.mem.Rows(table)); n != 10 {
		t.Fatalf("resume after 20 should deliver 10 events, got %d", n)
	}
}

func TestSubscriptionErrorClasses(t *testing.T) {
	e := newEnv(t, 0)
	e.emit(5)
	sub, wm := e.subscription(500)
	cases := []struct {
		err  error
		want Class
	}{
		{status.Error(codes.Unavailable, "down"), ClassTransient},
		{status.Error(codes.ResourceExhausted, "limit"), ClassQuota},
		{status.Error(codes.PermissionDenied, "no access"), ClassConfig},
	}
	for _, c := range cases {
		e.ps.FailNextSubscribes(c.err)
		err := sub.Run(context.Background(), wm.Restart(), Start{Preset: tenant.Latest})
		if Classify(err) != c.want {
			t.Errorf("%v classified as %v, want %v", c.err, Classify(err), c.want)
		}
	}

	// Expired replay ID.
	e.ps.Expire(orgID, topic, 4)
	err := sub.Run(context.Background(), wm.Restart(), Start{ReplayID: fakepubsub.ReplayID(1)})
	if Classify(err) != ClassReplayExpired {
		t.Errorf("expired replay classified as %v (%v)", Classify(err), err)
	}

	// Revoked session: auth error, and the token source logs in again next time.
	logins := e.oauth.Logins()
	e.oauth.RevokeAll()
	err = sub.Run(context.Background(), wm.Restart(), Start{Preset: tenant.Latest})
	if Classify(err) != ClassAuth {
		t.Errorf("revoked token classified as %v (%v)", Classify(err), err)
	}
	cancel, done := runAsync(sub, wm.Restart(), Start{Preset: tenant.Latest})
	waitFor(t, "stream", func() bool { return e.ps.ActiveStreams() == 1 })
	cancel()
	<-done
	if e.oauth.Logins() != logins+1 {
		t.Errorf("expected a fresh login after UNAUTHENTICATED")
	}

	// Server kills the stream mid-flight.
	waitFor(t, "previous stream closed", func() bool { return e.ps.ActiveStreams() == 0 })
	cancel, done = runAsync(sub, wm.Restart(), Start{Preset: tenant.Latest})
	waitFor(t, "stream", func() bool { return e.ps.ActiveStreams() == 1 })
	if n := e.ps.KillStreams(orgID, status.Error(codes.Internal, "boom")); n != 1 {
		t.Fatalf("killed %d streams", n)
	}
	if err := <-done; Classify(err) != ClassTransient {
		t.Errorf("killed stream classified as %v (%v)", Classify(err), err)
	}
	cancel()
}

func TestSubscriptionKeepaliveAdvancesIdle(t *testing.T) {
	e := newEnv(t, 0)
	e.ps.Keepalive = 20 * time.Millisecond
	e.emit(5)
	sub, wm := e.subscription(500)
	cancel, done := runAsync(sub, wm.Restart(), Start{Preset: tenant.Latest})
	waitFor(t, "idle advance", func() bool { return bytes.Equal(wm.Acked(), fakepubsub.ReplayID(5)) })
	cancel()
	<-done
	if len(e.mem.Rows(table)) != 0 {
		t.Error("LATEST should deliver nothing")
	}
}

func TestSubscriptionSinkResetAndOrgMismatch(t *testing.T) {
	e := newEnv(t, 50*time.Millisecond)
	sub, wm := e.subscription(500)
	cancel, done := runAsync(sub, wm.Restart(), Start{Preset: tenant.Latest})
	waitFor(t, "stream", func() bool { return e.ps.ActiveStreams() == 1 })
	e.emit(20)
	waitFor(t, "in flight", func() bool { return sub.Stats.Submitted.Load() > 0 })
	e.mem.FailStreams(table, errors.New("zerobus: stream failed"))
	select {
	case err := <-done:
		// Depending on timing the subscription sees the reset signal or a
		// rejected submit; the runner resumes from acked for both.
		if c := Classify(err); c != ClassSinkReset && c != ClassSinkUnavailable {
			t.Fatalf("got %v, want sink reset/unavailable", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("subscription did not observe the sink reset")
	}
	cancel()

	sub.ExpectedOrgID = "00DZZZZZZZZZZZZZZZ"
	if err := sub.Run(context.Background(), wm.Restart(), Start{Preset: tenant.Latest}); Classify(err) != ClassConfig {
		t.Fatalf("org mismatch = %v", err)
	}
}
