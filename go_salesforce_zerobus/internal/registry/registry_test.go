package registry

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/tenant"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/testutil/pgtest"
)

const doc = `
version: 1
defaults:
  subscription: { batch_size: 50 }
tenants:
  - key: acme
    org_id: 00D5g000000AbCdEAA
    labels: { tier: gold }
    salesforce:
      instance_url: https://acme.my.salesforce.com
      auth: { type: oauth_client_credentials, client_id: cid, client_secret_ref: "uc-secret://main.salesforce_zerobus_secrets.acme_prod#client_secret" }
    subscriptions:
      - topic: /data/ChangeEvents
      - topic: /data/OpportunityChangeEvent
        table: main.sales.opp_cdc
  - key: globex
    salesforce:
      instance_url: https://globex.my.salesforce.com
      auth: { type: soap, username: u, password_ref: env://GLOBEX_PW }
    subscriptions: [ { topic: /data/AccountChangeEvent, enabled: false } ]
`

func newRegistry(t *testing.T) *Registry {
	pool, schema := pgtest.Pool(t)
	d := tenant.BuiltinDefaults()
	d.Table = "main.salesforce.cdc_events"
	r := &Registry{
		Pool: pool, Schema: schema, Defaults: d, Interval: 50 * time.Millisecond,
		Options: tenant.LoadOptions{SecretSchemes: []string{"env", "file", "uc-secret"}},
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if err := r.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.Init(context.Background()); err != nil {
		t.Fatalf("Init must be idempotent: %v", err)
	}
	return r
}

func TestApplyLoadWatch(t *testing.T) {
	r := newRegistry(t)
	ctx := context.Background()
	d, err := tenant.ParseDocument("tenants.yaml", []byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	res, err := r.Apply(ctx, d, false)
	if err != nil {
		t.Fatal(err)
	}
	if res.TenantsUpserted != 2 || res.SubscriptionsUpserted != 3 {
		t.Fatalf("apply = %+v", res)
	}

	snap, rowErrs, err := r.Load(ctx)
	if err != nil || len(rowErrs) != 0 {
		t.Fatalf("load: %v %v", err, rowErrs)
	}
	subs := snap.Subscriptions()
	if len(subs) != 2 {
		t.Fatalf("want 2 enabled subscriptions, got %+v", subs)
	}
	if subs[0].Key.Topic != "/data/ChangeEvents" || subs[0].Table != "main.salesforce.cdc_events" || subs[0].BatchSize != 50 {
		t.Errorf("sub0 = %+v (service default table, file default batch size)", subs[0])
	}
	if subs[1].Table != "main.sales.opp_cdc" {
		t.Errorf("sub1 table = %s", subs[1].Table)
	}
	acme, _ := snap.Tenant("acme")
	if acme.Labels["tier"] != "gold" || acme.Salesforce.Auth.ClientSecretRef != "uc-secret://main.salesforce_zerobus_secrets.acme_prod#client_secret" {
		t.Errorf("acme = %+v", acme)
	}

	// Watch picks up a new tenant inserted with plain SQL, and flags an invalid one.
	var invalid []RowError
	r.OnInvalid = func(e []RowError) { invalid = e }
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch, err := r.Watch(wctx)
	if err != nil {
		t.Fatal(err)
	}
	first := <-ch
	if first.Version != snap.Version {
		t.Fatalf("first snapshot version %s != %s", first.Version, snap.Version)
	}
	_, err = r.Pool.Exec(ctx, `INSERT INTO `+r.q("tenants")+` (tenant_key, instance_url, auth_type, client_id, client_secret_ref)
VALUES ('initech', 'https://initech.my.salesforce.com', 'oauth_client_credentials', 'cid', 'env://INITECH'),
       ('broken', 'https://broken.my.salesforce.com', 'oauth_client_credentials', 'cid', NULL)`)
	if err != nil {
		t.Fatal(err)
	}
	r.Pool.Exec(ctx, `INSERT INTO `+r.q("subscriptions")+` (tenant_key, topic) VALUES ('initech', '/data/LeadChangeEvent'), ('broken', '/data/LeadChangeEvent')`)
	var next tenant.Snapshot
	select {
	case next = <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("watch did not emit after insert")
	}
	// The two inserts may arrive as one or two snapshots; drain to the latest.
	deadline := time.After(time.Second)
	for len(next.Subscriptions()) < 3 {
		select {
		case next = <-ch:
		case <-deadline:
			t.Fatalf("latest snapshot has %d subscriptions", len(next.Subscriptions()))
		}
	}
	if _, ok := next.Tenant("initech"); !ok {
		t.Error("initech not loaded")
	}
	if _, ok := next.Tenant("broken"); ok {
		t.Error("invalid tenant must be skipped")
	}
	if len(invalid) != 1 || invalid[0].Tenant != "broken" || !strings.Contains(invalid[0].Err.Error(), "client_secret_ref is required") {
		t.Errorf("invalid rows = %+v", invalid)
	}

	if err := r.SetEnabled(ctx, "acme", false); err != nil {
		t.Fatal(err)
	}
	if err := r.SetEnabled(ctx, "nope", false); err == nil {
		t.Error("SetEnabled on unknown tenant should fail")
	}

	// Prune removes tenants absent from the document.
	res, err = r.Apply(ctx, d, true)
	if err != nil || res.TenantsDeleted != 2 {
		t.Fatalf("prune: %+v %v", res, err)
	}
}

func TestApplyRejectsInvalidDocument(t *testing.T) {
	r := newRegistry(t)
	d, _ := tenant.ParseDocument("bad.yaml", []byte(`
version: 1
tenants:
  - key: acme
    salesforce: { instance_url: https://a.example.com, auth: { type: soap, username: u, password_ref: vault://x } }
    subscriptions: [ { topic: /data/X } ]`))
	if _, err := r.Apply(context.Background(), d, false); err == nil || !strings.Contains(err.Error(), "unknown secret scheme") {
		t.Fatalf("err = %v", err)
	}
	if v, _ := r.Version(context.Background()); v != 0 {
		t.Errorf("nothing should be written, version = %d", v)
	}
}

func TestStatus(t *testing.T) {
	r := newRegistry(t)
	ctx := context.Background()
	d, _ := tenant.ParseDocument("tenants.yaml", []byte(doc))
	r.Apply(ctx, d, false)
	now := time.Now()
	err := r.WriteStatus(ctx, []Status{
		{Tenant: "acme", Topic: "/data/ChangeEvents", State: "running", OrgID: "00D1", Owner: "pod-0", EventsAcked: 10, LastAckAt: now},
		{Tenant: "gone", Topic: "/data/X", State: "failed", Detail: "boom"},
	})
	if err != nil {
		t.Fatal(err)
	}
	r.WriteStatus(ctx, []Status{{Tenant: "acme", Topic: "/data/ChangeEvents", State: "backoff", Detail: "unavailable", EventsAcked: 12}})
	var state, org string
	var acked int64
	if err := r.Pool.QueryRow(ctx, "SELECT state, org_id, events_acked FROM "+r.q("subscription_status")+" WHERE tenant_key='acme'").Scan(&state, &org, &acked); err != nil {
		t.Fatal(err)
	}
	if state != "backoff" || org != "00D1" || acked != 12 {
		t.Errorf("status = %s %s %d (org_id should be kept when not reported)", state, org, acked)
	}
	if err := r.PruneStatus(ctx); err != nil {
		t.Fatal(err)
	}
	var n int
	r.Pool.QueryRow(ctx, "SELECT count(*) FROM "+r.q("subscription_status")).Scan(&n)
	if n != 1 {
		t.Errorf("prune left %d rows, want 1", n)
	}
}

func TestMaterialize(t *testing.T) {
	d := tenant.Defaults{Table: "a.b.c", APIVersion: "60.0", Subscription: tenant.SubscriptionInput{BatchSize: 25}}
	in := materialize(tenant.TenantInput{Subscriptions: []tenant.SubscriptionInput{{Topic: "/data/X"}, {Topic: "/data/Y", Table: "x.y.z", BatchSize: 5}}}, d)
	if in.Salesforce.APIVersion != "60.0" || in.Subscriptions[0].Table != "a.b.c" || in.Subscriptions[0].BatchSize != 25 ||
		in.Subscriptions[1].Table != "x.y.z" || in.Subscriptions[1].BatchSize != 5 || in.Subscriptions[0].MaxUnacked != 0 {
		t.Fatalf("materialize = %+v", in)
	}
}
