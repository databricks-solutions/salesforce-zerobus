package lakebase

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	sdktime "github.com/databricks/databricks-sdk-go/common/types/time"
	"github.com/databricks/databricks-sdk-go/service/postgres"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/checkpoint"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/testutil/pgtest"
)

func TestStoreSQL(t *testing.T) {
	pool, schema := pgtest.Pool(t)
	ctx := context.Background()
	s := New(pool, schema)
	if err := s.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Init(ctx); err != nil {
		t.Fatalf("Init must be idempotent: %v", err)
	}
	a := checkpoint.Key{Tenant: "acme", Topic: "/data/A"}
	b := checkpoint.Key{Tenant: "acme", Topic: "/data/B"}
	c := checkpoint.Key{Tenant: "globex", Topic: "/data/A"}
	err := s.SaveMany(ctx, []checkpoint.Checkpoint{
		{Key: a, OrgID: "00D1", Table: "x.y.z", ReplayID: []byte{1}, EventsAcked: 5, Owner: "pod-0"},
		{Key: c, OrgID: "00D2", Table: "x.y.z", ReplayID: []byte{9}, EventsAcked: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SaveMany(ctx, []checkpoint.Checkpoint{{Key: a, OrgID: "00D1", Table: "x.y.z", ReplayID: []byte{2}, EventsAcked: 3}}); err != nil {
		t.Fatal(err)
	}
	got, err := s.LoadMany(ctx, []checkpoint.Key{a, b})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("LoadMany returned %d rows, want 1 (b missing, c not requested)", len(got))
	}
	if cp := got[a]; !bytes.Equal(cp.ReplayID, []byte{2}) || cp.EventsAcked != 8 || cp.OrgID != "00D1" {
		t.Fatalf("a = %+v", cp)
	}
	if err := s.Delete(ctx, a); err != nil {
		t.Fatal(err)
	}
	got, _ = s.LoadMany(ctx, []checkpoint.Key{a, c})
	if _, ok := got[a]; ok || len(got) != 1 {
		t.Fatalf("after delete: %+v", got)
	}
}

type fakeCreds struct {
	mu       sync.Mutex
	calls    int
	fail     int // fail this many calls before succeeding
	lifetime time.Duration
	now      func() time.Time
	lastReq  postgres.GenerateDatabaseCredentialRequest
	delay    time.Duration
}

func (f *fakeCreds) GenerateDatabaseCredential(_ context.Context, req postgres.GenerateDatabaseCredentialRequest) (*postgres.DatabaseCredential, error) {
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.lastReq = req
	if f.fail > 0 {
		f.fail--
		return nil, errors.New("Invalid Token")
	}
	return &postgres.DatabaseCredential{Token: fmt.Sprintf("tok-%d", f.calls), ExpireTime: sdktime.New(f.now().Add(f.lifetime))}, nil
}

func (f *fakeCreds) GetEndpoint(context.Context, postgres.GetEndpointRequest) (*postgres.Endpoint, error) {
	return &postgres.Endpoint{Status: &postgres.EndpointStatus{Hosts: &postgres.EndpointHosts{Host: "h"}}}, nil
}

func (f *fakeCreds) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestTokenCacheRefreshAtSeventyFivePercentAndFallback(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	clock := func() time.Time { return now }
	f := &fakeCreds{lifetime: time.Hour, now: clock}
	tc := newTokenCache(f, "projects/p/branches/b/endpoints/e", time.Hour, quiet())
	tc.now = clock
	ctx := context.Background()

	tok, err := tc.token(ctx)
	if err != nil || tok != "tok-1" {
		t.Fatalf("first token = %q, %v", tok, err)
	}
	now = now.Add(44 * time.Minute)
	if tok, _ := tc.token(ctx); tok != "tok-1" || f.count() != 1 {
		t.Fatalf("before 75%% of lifetime the cached credential is used (tok=%s calls=%d)", tok, f.count())
	}
	now = now.Add(2 * time.Minute) // 46m: past the 45m refresh point
	if tok, _ := tc.token(ctx); tok != "tok-2" {
		t.Fatalf("expected refresh after 45m, got %s", tok)
	}
	if f.lastReq.Ttl != nil {
		t.Errorf("1h TTL is the API default and should not be sent: %+v", f.lastReq.Ttl)
	}

	// Refresh fails while the current credential is still valid: keep using it.
	now = now.Add(50 * time.Minute) // 4m before tok-2 expires
	f.fail = 1
	if tok, err := tc.token(ctx); err != nil || tok != "tok-2" {
		t.Fatalf("fallback = %q, %v", tok, err)
	}
	// Past expiry and the refresh still fails: error, never an expired token.
	now = now.Add(10 * time.Minute)
	f.fail = 1
	if tok, err := tc.token(ctx); err == nil || tok != "" {
		t.Fatalf("expired credential must not be returned: %q, %v", tok, err)
	}
	if tok, err := tc.token(ctx); err != nil || tok == "tok-2" {
		t.Fatalf("recovered = %q, %v", tok, err)
	}
}

func TestTokenCacheRequestsShortTTL(t *testing.T) {
	f := &fakeCreds{lifetime: 10 * time.Minute, now: time.Now}
	tc := newTokenCache(f, "projects/p/branches/b/endpoints/e", 10*time.Minute, quiet())
	if _, err := tc.token(context.Background()); err != nil {
		t.Fatal(err)
	}
	if f.lastReq.Ttl == nil || f.lastReq.Ttl.AsDuration() != 10*time.Minute {
		t.Fatalf("ttl = %+v", f.lastReq.Ttl)
	}
	if d := tc.expires.Sub(tc.issued); d < 9*time.Minute || d > 11*time.Minute {
		t.Fatalf("lifetime = %v", d)
	}
}

func TestTokenCacheConcurrentCallersShareOneMint(t *testing.T) {
	f := &fakeCreds{lifetime: time.Hour, now: time.Now, delay: 50 * time.Millisecond}
	tc := newTokenCache(f, "projects/p/branches/b/endpoints/e", time.Hour, quiet())
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if tok, err := tc.token(context.Background()); err != nil || tok == "" {
				t.Errorf("token = %q, %v", tok, err)
			}
		}()
	}
	wg.Wait()
	if f.count() != 1 {
		t.Fatalf("mints = %d, want 1", f.count())
	}
}

func TestRefreshLoopRenewsAndRetries(t *testing.T) {
	f := &fakeCreds{lifetime: 400 * time.Millisecond, now: time.Now}
	tc := newTokenCache(f, "projects/p/branches/b/endpoints/e", 5*time.Minute, quiet())
	tc.retry.Base, tc.retry.Max = 20*time.Millisecond, 50*time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := tc.token(ctx); err != nil {
		t.Fatal(err)
	}
	go tc.refreshLoop(ctx)
	deadline := time.Now().Add(3 * time.Second)
	for f.count() < 3 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if f.count() < 3 {
		t.Fatalf("background loop refreshed %d times, want >= 2 renewals", f.count()-1)
	}
	// Failures are retried with backoff until a mint succeeds.
	f.mu.Lock()
	f.fail = 3
	before := f.calls
	f.mu.Unlock()
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		f.mu.Lock()
		recovered := f.calls >= before+4 && f.fail == 0
		f.mu.Unlock()
		if recovered {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if tok, err := tc.token(ctx); err != nil || tok == "" {
		t.Fatalf("after retries: %q, %v", tok, err)
	}
	cancel()
}
