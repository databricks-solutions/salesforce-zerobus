package lakebase

import (
	"bytes"
	"context"
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

type fakeCreds struct{ calls int }

func (f *fakeCreds) GenerateDatabaseCredential(context.Context, postgres.GenerateDatabaseCredentialRequest) (*postgres.DatabaseCredential, error) {
	f.calls++
	return &postgres.DatabaseCredential{Token: "tok", ExpireTime: sdktime.New(time.Unix(3600, 0))}, nil
}

func (f *fakeCreds) GetEndpoint(context.Context, postgres.GetEndpointRequest) (*postgres.Endpoint, error) {
	return &postgres.Endpoint{Status: &postgres.EndpointStatus{Hosts: &postgres.EndpointHosts{Host: "h"}}}, nil
}

func TestTokenCacheRefreshesBeforeExpiry(t *testing.T) {
	f := &fakeCreds{}
	now := time.Unix(0, 0)
	tc := &tokenCache{api: f, endpoint: "projects/p/branches/b/endpoints/e", now: func() time.Time { return now }}
	for i := 0; i < 3; i++ {
		if tok, err := tc.token(context.Background()); err != nil || tok != "tok" {
			t.Fatal(tok, err)
		}
	}
	if f.calls != 1 {
		t.Fatalf("calls = %d, want cached", f.calls)
	}
	now = time.Unix(3600-10*60, 0) // within 15 minutes of expiry
	tc.token(context.Background())
	if f.calls != 2 {
		t.Fatalf("calls = %d, want refresh near expiry", f.calls)
	}
}
