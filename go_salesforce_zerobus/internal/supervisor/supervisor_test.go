package supervisor

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/checkpoint"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/pubsub"
	zbsink "github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/sink/zerobus"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/tenant"
)

// Statuses must survive shutdown so the final "stopped" status can be
// written after every runner has exited.
func TestFinalStatusesAfterStop(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	snk := zbsink.New(zbsink.NewMemoryFactory(0), zbsink.Options{Logger: logger})
	defer snk.Close(context.Background())
	conns := pubsub.NewConnPool("127.0.0.1:1", 10, true) // unreachable: runners just retry
	defer conns.Close()
	sup := New(Deps{
		Shared: &pubsub.Shared{Conns: conns, Schemas: pubsub.NewSchemaCache(0), Logger: logger},
		Sink:   snk, Registry: checkpoint.NewRegistry(), Store: checkpoint.NewMemoryStore(), Logger: logger,
		Resolve: func(context.Context, string) (string, error) { return "x", nil },
	})
	d := tenant.BuiltinDefaults()
	d.Table = "a.b.c"
	tn, err := tenant.Build(tenant.TenantInput{Key: "acme", Salesforce: tenant.Salesforce{InstanceURL: "http://127.0.0.1:1",
		Auth: tenant.Auth{Type: tenant.AuthOAuthClientCredentials, ClientID: "c", ClientSecretRef: "env://S"}},
		Subscriptions: []tenant.SubscriptionInput{{Topic: "/data/X"}}}, d, "t", tenant.LoadOptions{SecretSchemes: []string{"env"}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	snaps := make(chan tenant.Snapshot, 1)
	snaps <- tenant.Snapshot{Tenants: []tenant.Tenant{tn}}
	done := make(chan error)
	go func() { done <- sup.Run(ctx, snaps) }()
	deadline := time.Now().Add(5 * time.Second)
	for len(sup.Statuses()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if len(sup.FinalStatuses()) != 0 {
		t.Fatal("FinalStatuses should be empty while running")
	}
	cancel()
	<-done
	if len(sup.Statuses()) != 0 {
		t.Error("no runners after stop")
	}
	final := sup.FinalStatuses()
	if len(final) != 1 || final[0].Tenant != "acme" || final[0].Topic != "/data/X" {
		t.Fatalf("FinalStatuses = %+v", final)
	}
}
