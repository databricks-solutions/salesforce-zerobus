package schemareg

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/testutil/cdc"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/testutil/pgtest"
)

var logger = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestEntityName(t *testing.T) {
	cases := map[string]string{
		cdc.AccountSchemaJSON:                                            "Account",
		`{"type":"record","name":"Invoice__ChangeEvent"}`:                "Invoice__c",
		`{"type":"record","name":"ns__Invoice__ChangeEvent"}`:            "ns__Invoice__c",
		`{"type":"record","name":"com.sforce.eventbus.CaseChangeEvent"}`: "Case",
		`{"type":"record","name":"Order_Shipped__e"}`:                    "Order_Shipped__e",
		`not json`: "",
	}
	for schema, want := range cases {
		if got := EntityName(schema); got != want {
			t.Errorf("EntityName(%.40s) = %q, want %q", schema, got, want)
		}
	}
}

func TestPutBoundsPending(t *testing.T) {
	s := New(nil, "", logger) // Put never touches the database
	big := strings.Repeat("x", maxPendingBytes/2)
	s.Put(Record{OrgID: "a", SchemaID: "1", JSON: big})
	s.Put(Record{OrgID: "a", SchemaID: "1", JSON: big}) // same key: not counted twice
	s.Put(Record{OrgID: "a", SchemaID: "2", JSON: big})
	s.Put(Record{OrgID: "a", SchemaID: "3", JSON: big}) // over budget: dropped
	if s.Pending() != 2 || s.pendingBytes != 2*len(big) {
		t.Fatalf("pending=%d bytes=%d", s.Pending(), s.pendingBytes)
	}
	if b := s.nextBatch(); len(b) != 1 {
		t.Fatalf("batches are capped at %d bytes: got %d records", maxBatchBytes, len(b))
	}
}

func TestStoreSQL(t *testing.T) {
	pool, schema := pgtest.Pool(t)
	ctx := context.Background()
	s := New(pool, schema, logger)
	if err := s.Init(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Init(ctx); err != nil {
		t.Fatalf("Init must be idempotent: %v", err)
	}
	if _, ok, err := s.Get(ctx, "00D1", "s1"); ok || err != nil {
		t.Fatalf("empty registry: ok=%v err=%v", ok, err)
	}
	s.Put(Record{OrgID: "00D1", SchemaID: "s1", TenantKey: "acme", Topic: "/data/AccountChangeEvent", JSON: cdc.AccountSchemaJSON})
	s.Put(Record{OrgID: "00D2", SchemaID: "s1", JSON: `{"type":"record","name":"Invoice__ChangeEvent"}`})
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if s.Pending() != 0 {
		t.Fatalf("pending after flush = %d", s.Pending())
	}
	js, ok, err := s.Get(ctx, "00D1", "s1")
	if err != nil || !ok || js != cdc.AccountSchemaJSON {
		t.Fatalf("Get = %.30q %v %v", js, ok, err)
	}

	// Schemas are immutable: another replica's write of the same key is a no-op.
	s.Put(Record{OrgID: "00D1", SchemaID: "s1", JSON: `{"changed":true}`})
	if err := s.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	var entity, tenantKey string
	var n int
	err = pool.QueryRow(ctx, "SELECT count(*), max(entity_name) FILTER (WHERE org_id = '00D1'), max(tenant_key) FROM "+s.table).Scan(&n, &entity, &tenantKey)
	if err != nil || n != 2 || entity != "Account" || tenantKey != "acme" {
		t.Fatalf("rows=%d entity=%q tenant=%q err=%v", n, entity, tenantKey, err)
	}
	if js, _, _ := s.Get(ctx, "00D1", "s1"); js != cdc.AccountSchemaJSON {
		t.Fatalf("first write must win, got %.30q", js)
	}

	// Run writes queued schemas promptly.
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { s.Run(runCtx); close(done) }()
	s.Put(Record{OrgID: "00D3", SchemaID: "s9", JSON: cdc.AccountSchemaJSON})
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, ok, _ := s.Get(ctx, "00D3", "s9"); ok {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Run did not write the queued schema")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
}

// TestInitOnOperatorCreatedTable mirrors a registry table created by a
// person rather than the service principal: Init must not need ownership,
// and must say which GRANT is missing.
func TestInitOnOperatorCreatedTable(t *testing.T) {
	pool, schema := pgtest.Pool(t)
	ctx := context.Background()
	role := fmt.Sprintf("sfzb_test_sp_%d", time.Now().UnixNano())
	qs, qr := pgx.Identifier{schema}.Sanitize(), pgx.Identifier{role}.Sanitize()
	var db string
	if err := pool.QueryRow(ctx, "SELECT current_database()").Scan(&db); err != nil {
		t.Fatal(err)
	}
	for _, q := range []string{
		"CREATE ROLE " + qr + " LOGIN",
		"GRANT CONNECT, CREATE ON DATABASE " + pgx.Identifier{db}.Sanitize() + " TO " + qr, // as zerobus bootstrap lakebase does
		"CREATE SCHEMA " + qs,
		"GRANT USAGE, CREATE ON SCHEMA " + qs + " TO " + qr,
	} {
		if _, err := pool.Exec(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	operator := New(pool, schema, logger)
	if err := operator.Init(ctx); err != nil { // creates the table and index as the operator
		t.Fatal(err)
	}

	cfg, err := pgxpool.ParseConfig(os.Getenv("SFZB_TEST_PG_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.User = role
	spPool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		spPool.Close()
		pool.Exec(context.Background(), "DROP OWNED BY "+qr)
		pool.Exec(context.Background(), "DROP ROLE "+qr)
	})
	sp := New(spPool, schema, logger)
	err = sp.Init(ctx)
	if err == nil || !strings.Contains(err.Error(), "GRANT SELECT, INSERT ON "+operator.table+" TO "+qr) {
		t.Fatalf("Init without privileges should name the missing grant, got %v", err)
	}
	if _, err := pool.Exec(ctx, "GRANT SELECT, INSERT ON "+operator.table+" TO "+qr); err != nil {
		t.Fatal(err)
	}
	if err := sp.Init(ctx); err != nil {
		t.Fatalf("Init after the grant: %v", err)
	}
	sp.Put(Record{OrgID: "00D1", SchemaID: "s1", JSON: cdc.AccountSchemaJSON})
	if err := sp.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := sp.Get(ctx, "00D1", "s1"); !ok || err != nil {
		t.Fatalf("Get after grant: ok=%v err=%v", ok, err)
	}
}
