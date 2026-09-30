package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/backoff"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/checkpoint"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/config"
	zbsink "github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/sink/zerobus"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/supervisor"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/tenant"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/testutil/cdc"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/testutil/fakeoauth"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/testutil/fakepubsub"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/proto/eventpb"
)

const (
	e2eTopic = "/data/AccountChangeEvent"
	e2eTable = "main.sf.cdc_events"
)

type harness struct {
	t      *testing.T
	oauth  *fakeoauth.Server
	ps     *fakepubsub.Server
	orgs   []string
	snap   tenant.Snapshot
	schema string
}

func newHarness(t *testing.T, n int) *harness {
	t.Setenv("SFZB_E2E_SECRET", "secret")
	h := &harness{t: t, oauth: fakeoauth.New()}
	t.Cleanup(h.oauth.Close)
	ps, err := fakepubsub.Start(h.oauth.OrgForToken)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(ps.Stop)
	h.ps = ps
	h.schema = cdc.SchemaID(cdc.AccountSchemaJSON)
	ps.AddSchema(h.schema, cdc.AccountSchemaJSON)
	for i := 0; i < n; i++ {
		org := fmt.Sprintf("00D%012dAAA", i)
		h.orgs = append(h.orgs, org)
		h.oauth.AddClient(fmt.Sprintf("client-%d", i), "secret", org)
		in := tenant.TenantInput{
			Key: fmt.Sprintf("tenant-%03d", i), OrgID: org,
			Salesforce: tenant.Salesforce{InstanceURL: h.oauth.URL, Auth: tenant.Auth{
				Type: tenant.AuthOAuthClientCredentials, ClientID: fmt.Sprintf("client-%d", i), ClientSecretRef: "env://SFZB_E2E_SECRET"}},
			Subscriptions: []tenant.SubscriptionInput{{Topic: e2eTopic, ReplayDefault: tenant.Earliest, BatchSize: 20, MaxUnacked: 60}},
		}
		d := tenant.BuiltinDefaults()
		d.Table = e2eTable
		tn, err := tenant.Build(in, d, "e2e", tenant.LoadOptions{SecretSchemes: []string{"env"}})
		if err != nil {
			t.Fatal(err)
		}
		h.snap.Tenants = append(h.snap.Tenants, tn)
	}
	h.snap.Version = "e2e"
	return h
}

func (h *harness) emit(perOrg int) {
	for _, org := range h.orgs {
		for i := 0; i < perOrg; i++ {
			p, err := cdc.EncodeAccount(cdc.AccountEvent{RecordID: "001" + org, ChangeType: "UPDATE", Name: "n", CommitNumber: int64(i)})
			if err != nil {
				h.t.Fatal(err)
			}
			h.ps.Emit(org, e2eTopic, h.schema, p)
		}
	}
}

type running struct {
	cancel context.CancelFunc
	done   chan error
	addr   string
}

func (h *harness) start(store checkpoint.Store, mem *zbsink.MemoryFactory) *running {
	cfg, err := config.Load([]string{
		"-tenant-source", "file", "-tenants-file", "unused.yaml", "-sink", "memory", "-checkpoint-store", "memory",
		"-sf-pubsub-addr", h.ps.Addr, "-sf-insecure", "-admin-addr", "127.0.0.1:0", "-startup-spread", "0",
		"-checkpoint-interval", "50ms", "-zb-batch-linger", "2ms", "-auth-rps", "0", "-subscribe-rps", "0", "-schema-rps", "0",
	}, func(string) string { return "" })
	if err != nil {
		h.t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fast := backoff.Policy{Base: 10 * time.Millisecond, Max: 50 * time.Millisecond}
	ready := make(chan string, 1)
	ctx, cancel := context.WithCancel(context.Background())
	r := &running{cancel: cancel, done: make(chan error, 1)}
	go func() {
		r.done <- Run(ctx, Options{
			Config: cfg, Logger: logger, Version: "test", Factory: mem, Store: store,
			Source: tenant.StaticSource{Snapshot: h.snap}, Ready: func(a string) { ready <- a },
			Policy: &supervisor.Policy{Transient: fast, Quota: fast, Sink: fast, Failed: time.Second, HealthyAfter: time.Minute},
		})
	}()
	select {
	case r.addr = <-ready:
	case err := <-r.done:
		h.t.Fatalf("Run exited early: %v", err)
	case <-time.After(10 * time.Second):
		h.t.Fatal("service did not start")
	}
	return r
}

func (r *running) stop(t *testing.T) {
	r.cancel()
	select {
	case err := <-r.done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("shutdown timed out")
	}
}

func waitFor(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// delivered decodes rows into org -> set of replay positions.
func delivered(t *testing.T, mem *zbsink.MemoryFactory) (map[string]map[int]int, int) {
	out := map[string]map[int]int{}
	rows := mem.Rows(e2eTable)
	for _, raw := range rows {
		var row eventpb.SalesforceEvent
		if err := proto.Unmarshal(raw, &row); err != nil {
			t.Fatal(err)
		}
		var pos int
		fmt.Sscanf(row.GetReplayId(), "%x", &pos)
		if out[row.GetOrgId()] == nil {
			out[row.GetOrgId()] = map[int]int{}
		}
		out[row.GetOrgId()][pos]++
	}
	return out, len(rows)
}

func (h *harness) complete(store *checkpoint.MemoryStore, perOrg int) bool {
	for i := range h.orgs {
		cp, ok := store.Get(checkpoint.Key{Tenant: fmt.Sprintf("tenant-%03d", i), Topic: e2eTopic})
		if !ok {
			return false
		}
		if pos, _ := fakepubsub.Position(cp.ReplayID); pos != perOrg {
			return false
		}
	}
	return true
}

func TestEndToEndMultiTenantWithFailuresAndRestart(t *testing.T) {
	baseline := runtime.NumGoroutine()
	h := newHarness(t, 30)
	store := checkpoint.NewMemoryStore()
	mem := zbsink.NewMemoryFactory(2 * time.Millisecond)
	h.emit(40)

	r := h.start(store, mem)
	resp, err := http.Get("http://" + r.addr + "/healthz")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("healthz: %v %v", resp, err)
	}
	waitFor(t, "ready", 10*time.Second, func() bool {
		resp, err := http.Get("http://" + r.addr + "/readyz")
		return err == nil && resp.StatusCode == 200
	})

	// Inject failures while events keep flowing.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			h.emit(8)
			time.Sleep(30 * time.Millisecond)
			if i%2 == 0 {
				mem.FailStreams(e2eTable, errors.New("injected zerobus failure"))
			} else {
				h.ps.KillStreams("", status.Error(codes.Unavailable, "injected salesforce failure"))
			}
		}
	}()
	wg.Wait()
	const total = 40 + 5*8
	waitFor(t, "all checkpoints at the last event", 30*time.Second, func() bool { return h.complete(store, total) })
	r.stop(t)

	if mem.Opened() < 2 || h.ps.Subscribes() <= int64(len(h.orgs)) {
		t.Fatalf("failures were not exercised: zerobus opens=%d salesforce subscribes=%d", mem.Opened(), h.ps.Subscribes())
	}
	got, rows := delivered(t, mem)
	for _, org := range h.orgs {
		for pos := 1; pos <= total; pos++ {
			if got[org][pos] == 0 {
				t.Fatalf("org %s: event %d never delivered (gap)", org, pos)
			}
		}
	}
	t.Logf("first run: %d rows for %d events (%.1f%% duplicates from injected failures)", rows, total*len(h.orgs),
		100*float64(rows-total*len(h.orgs))/float64(total*len(h.orgs)))

	// Restart from stored checkpoints: only new events are delivered.
	h.emit(10)
	mem2 := zbsink.NewMemoryFactory(0)
	r = h.start(store, mem2)
	waitFor(t, "restart caught up", 30*time.Second, func() bool { return h.complete(store, total+10) })
	r.stop(t)
	got2, rows2 := delivered(t, mem2)
	if rows2 != 10*len(h.orgs) {
		t.Errorf("after a clean shutdown the restart should deliver exactly the new events: got %d rows, want %d", rows2, 10*len(h.orgs))
	}
	for _, org := range h.orgs {
		for pos := range got2[org] {
			if pos <= total {
				t.Errorf("org %s: event %d redelivered after clean restart", org, pos)
			}
		}
	}

	// Nothing from the service outlives Run (fake servers are stopped by cleanup).
	_ = baseline
	deadline := time.Now().Add(10 * time.Second)
	for {
		leaked := serviceGoroutines()
		if leaked == "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("service goroutines still running after shutdown:\n%s", leaked)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// serviceGoroutines returns stacks of goroutines running service code (not
// test fixtures or the test itself).
func serviceGoroutines() string {
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, true)]
	var out []string
	for _, g := range strings.Split(string(buf), "\n\n") {
		if strings.Contains(g, "go_salesforce_zerobus/internal/") &&
			!strings.Contains(g, "internal/testutil/") && !strings.Contains(g, "internal/app.Test") &&
			!strings.Contains(g, "internal/app.serviceGoroutines") {
			out = append(out, g)
		}
	}
	return strings.Join(out, "\n\n")
}

func TestEndToEndDisabledTenantAndReconcile(t *testing.T) {
	h := newHarness(t, 3)
	h.snap.Tenants[2].Enabled = false
	store := checkpoint.NewMemoryStore()
	mem := zbsink.NewMemoryFactory(0)
	h.emit(5)
	r := h.start(store, mem)
	waitFor(t, "two tenants done", 10*time.Second, func() bool {
		_, ok := store.Get(checkpoint.Key{Tenant: "tenant-001", Topic: e2eTopic})
		_, ok0 := store.Get(checkpoint.Key{Tenant: "tenant-000", Topic: e2eTopic})
		return ok && ok0
	})
	resp, err := http.Get("http://" + r.addr + "/debug/subscriptions")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("debug: %v", err)
	}
	r.stop(t)
	if _, ok := store.Get(checkpoint.Key{Tenant: "tenant-002", Topic: e2eTopic}); ok {
		t.Error("disabled tenant should not run")
	}
}

func TestEndToEndExpiredReplayFallsBack(t *testing.T) {
	h := newHarness(t, 1)
	h.emit(20)
	h.ps.Expire(h.orgs[0], e2eTopic, 11) // events 1-10 aged out of retention
	store := checkpoint.NewMemoryStore()
	key := checkpoint.Key{Tenant: "tenant-000", Topic: e2eTopic}
	store.Put(checkpoint.Checkpoint{Key: key, OrgID: h.orgs[0], Table: e2eTable, ReplayID: fakepubsub.ReplayID(3)})
	mem := zbsink.NewMemoryFactory(0)
	r := h.start(store, mem)
	waitFor(t, "fallback caught up", 10*time.Second, func() bool {
		cp, _ := store.Get(key)
		pos, _ := fakepubsub.Position(cp.ReplayID)
		return pos == 20
	})
	r.stop(t)
	got, rows := delivered(t, mem)
	if rows != 10 || got[h.orgs[0]][11] == 0 {
		t.Fatalf("expected EARLIEST fallback to deliver retained events 11-20, got %d rows %v", rows, got)
	}
}

func TestEndToEndIgnoresCheckpointFromAnotherOrg(t *testing.T) {
	h := newHarness(t, 1)
	h.emit(5)
	store := checkpoint.NewMemoryStore()
	key := checkpoint.Key{Tenant: "tenant-000", Topic: e2eTopic}
	// e.g. the sandbox was refreshed and now has a new org ID
	store.Put(checkpoint.Checkpoint{Key: key, OrgID: "00DOLD000000000AAA", Table: e2eTable, ReplayID: fakepubsub.ReplayID(4)})
	mem := zbsink.NewMemoryFactory(0)
	r := h.start(store, mem)
	waitFor(t, "replayed from EARLIEST", 10*time.Second, func() bool {
		cp, _ := store.Get(key)
		pos, _ := fakepubsub.Position(cp.ReplayID)
		return pos == 5 && cp.OrgID == h.orgs[0]
	})
	r.stop(t)
	if _, rows := delivered(t, mem); rows != 5 {
		t.Fatalf("foreign checkpoint should be ignored (replay_default EARLIEST delivers 5), got %d", rows)
	}
}
