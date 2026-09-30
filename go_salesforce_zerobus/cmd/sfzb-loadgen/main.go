// Command sfzb-loadgen simulates many Salesforce orgs (fake OAuth + fake
// Pub/Sub API) generating CDC events, and either runs the service in-process
// against them with an in-memory Zerobus sink, reporting throughput and
// resource use, or writes a tenants file for running the real binary.
//
//	sfzb-loadgen -orgs 2000 -rate 2000 -duration 3m           # in-process capacity run
//	sfzb-loadgen -orgs 50 -serve -tenants-out /tmp/tenants.yaml # fakes only; then run `zerobus`
package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/app"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/checkpoint"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/config"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/obs"
	zbsink "github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/sink/zerobus"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/tenant"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/testutil/cdc"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/testutil/fakeoauth"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/testutil/fakepubsub"
)

func main() {
	orgs := flag.Int("orgs", 500, "simulated Salesforce orgs (one subscription each)")
	rateF := flag.Float64("rate", 500, "total events per second across all orgs")
	duration := flag.Duration("duration", 2*time.Minute, "run length (in-process mode)")
	ackLatency := flag.Duration("ack-latency", 20*time.Millisecond, "simulated Zerobus ack latency")
	serve := flag.Bool("serve", false, "only run the fake servers (use with -tenants-out)")
	tenantsOut := flag.String("tenants-out", "", "write a tenants YAML for the simulated orgs")
	authRPS := flag.Float64("auth-rps", 200, "service login rate limit (in-process)")
	logLevel := flag.String("log-level", "warn", "service log level (in-process)")
	spread := flag.Duration("startup-spread", 0, "service startup spread (in-process)")
	flag.Parse()

	oauth := fakeoauth.New()
	defer oauth.Close()
	ps, err := fakepubsub.Start(oauth.OrgForToken)
	if err != nil {
		fatal(err)
	}
	defer ps.Stop()
	schemaID := cdc.SchemaID(cdc.AccountSchemaJSON)
	ps.AddSchema(schemaID, cdc.AccountSchemaJSON)

	const topic = "/data/AccountChangeEvent"
	var inputs []tenant.TenantInput
	orgIDs := make([]string, *orgs)
	for i := range *orgs {
		org := fmt.Sprintf("00D%012dAAA", i)
		orgIDs[i] = org
		oauth.AddClient(fmt.Sprintf("client-%d", i), "secret", org)
		inputs = append(inputs, tenant.TenantInput{
			Key: fmt.Sprintf("org-%05d", i), OrgID: org,
			Salesforce: tenant.Salesforce{InstanceURL: oauth.URL, Auth: tenant.Auth{
				Type: tenant.AuthOAuthClientCredentials, ClientID: fmt.Sprintf("client-%d", i), ClientSecretRef: "env://SFZB_LOADGEN_SECRET"}},
			// EARLIEST so events emitted while subscriptions start up are
			// still delivered and counted.
			Subscriptions: []tenant.SubscriptionInput{{Topic: topic, ReplayDefault: tenant.Earliest}},
		})
	}
	os.Setenv("SFZB_LOADGEN_SECRET", "secret")
	if *tenantsOut != "" {
		if err := writeTenants(*tenantsOut, inputs); err != nil {
			fatal(err)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var emitted atomic.Int64
	produceCtx, stopProduce := context.WithCancel(ctx)
	defer stopProduce()
	go produce(produceCtx, ps, orgIDs, topic, schemaID, *rateF, &emitted)

	if *serve {
		fmt.Printf("fake Salesforce running for %d orgs\n  SFZB_SF_PUBSUB_ADDR=%s SFZB_SF_INSECURE=true SFZB_LOADGEN_SECRET=secret\n", *orgs, ps.Addr)
		if *tenantsOut != "" {
			fmt.Printf("  SFZB_TENANT_SOURCE=file SFZB_TENANTS_FILE=%s SFZB_DEFAULT_TABLE=main.load.cdc_events\n", *tenantsOut)
		}
		<-ctx.Done()
		return
	}

	// In-process service with an in-memory sink.
	d := tenant.BuiltinDefaults()
	d.Table = "main.load.cdc_events"
	var snap tenant.Snapshot
	for _, in := range inputs {
		t, err := tenant.Build(in, d, "loadgen", tenant.LoadOptions{SecretSchemes: []string{"env"}})
		if err != nil {
			fatal(err)
		}
		snap.Tenants = append(snap.Tenants, t)
	}
	snap.Version = "loadgen"
	cfg, err := config.Load([]string{
		"-tenant-source", "file", "-tenants-file", "unused", "-sink", "memory", "-checkpoint-store", "memory",
		"-sf-pubsub-addr", ps.Addr, "-sf-insecure", "-admin-addr", "127.0.0.1:0", "-log-level", *logLevel,
		"-auth-rps", fmt.Sprint(*authRPS), "-subscribe-rps", fmt.Sprint(*authRPS), "-schema-rps", fmt.Sprint(*authRPS),
		"-startup-spread", spread.String(),
	}, func(string) string { return "" })
	if err != nil {
		fatal(err)
	}
	logger, _ := obs.NewLogger(os.Stderr, *logLevel, "text")
	mem := zbsink.NewMemoryFactory(*ackLatency)
	mem.KeepRows = false
	store := checkpoint.NewMemoryStore()
	// Produce for -duration, drain, then stop by cancellation (like SIGTERM).
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	time.AfterFunc(*duration, stopProduce)
	done := make(chan error, 1)
	var adminAddr string
	ready := make(chan struct{})
	go func() {
		done <- app.Run(runCtx, app.Options{Config: cfg, Logger: logger, Version: "loadgen", Factory: mem, Store: store,
			Source: tenant.StaticSource{Snapshot: snap}, Ready: func(a string) { adminAddr = a; close(ready) }})
	}()
	<-ready
	fmt.Printf("service admin on http://%s  (orgs=%d, target %.0f events/s, ack latency %s)\n", adminAddr, *orgs, *rateF, *ackLatency)
	fmt.Println("elapsed  emitted  ingested  ingest/s  sf_streams  goroutines  heap_MiB  sys_MiB")

	start := time.Now()
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	poll := time.NewTicker(200 * time.Millisecond)
	defer poll.Stop()
	var lastIngested int64
	var peakHeap, peakSys uint64
	var allRunningAt, drainedAt time.Duration
	for {
		select {
		case err := <-done:
			if err != nil {
				fatal(err)
			}
			total := emitted.Load()
			fmt.Printf("\ndone: %d orgs, emitted %d, ingested %d (%.2f%%)\n", *orgs, total, mem.Ingested(), 100*float64(mem.Ingested())/float64(max(total, 1)))
			fmt.Printf("all subscriptions streaming after %s; backlog drained %s after producing stopped\n", allRunningAt.Round(time.Second), drainedAt.Round(time.Millisecond))
			fmt.Printf("peak heap %d MiB, peak Go sys %d MiB (includes the fake Salesforce servers)\n", peakHeap>>20, peakSys>>20)
			fmt.Printf("fake Pub/Sub: %s\n", ps)
			return
		case <-poll.C:
			if allRunningAt == 0 && ps.ActiveStreams() >= *orgs {
				allRunningAt = time.Since(start)
			}
			if produceCtx.Err() != nil && drainedAt == 0 && mem.Ingested() >= emitted.Load() {
				drainedAt = time.Since(start) - *duration
				cancel()
			}
		case <-tick.C:
			var ms runtime.MemStats
			runtime.ReadMemStats(&ms)
			peakHeap, peakSys = max(peakHeap, ms.HeapAlloc), max(peakSys, ms.Sys)
			ing := mem.Ingested()
			fmt.Printf("%7s  %7d  %8d  %8.0f  %10d  %10d  %8d  %7d\n",
				time.Since(start).Round(time.Second), emitted.Load(), ing, float64(ing-lastIngested)/10,
				ps.ActiveStreams(), runtime.NumGoroutine(), ms.HeapAlloc>>20, ms.Sys>>20)
			lastIngested = ing
		}
	}
}

// produce emits events round-robin across random orgs at rate per second.
func produce(ctx context.Context, ps *fakepubsub.Server, orgs []string, topic, schemaID string, rate float64, emitted *atomic.Int64) {
	if rate <= 0 {
		return
	}
	payload, _ := cdc.EncodeAccount(cdc.AccountEvent{RecordID: "001000000000001", ChangeType: "UPDATE", Name: "Acme Corp", City: "San Francisco", ChangedFields: []string{"0x0A", "3-0x02"}})
	interval := 10 * time.Millisecond
	perTick := rate * interval.Seconds()
	var carry float64
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		carry += perTick
		n := int(carry)
		carry -= float64(n)
		for range n {
			ps.Emit(orgs[rand.IntN(len(orgs))], topic, schemaID, payload)
		}
		emitted.Add(int64(n))
	}
}

func writeTenants(path string, inputs []tenant.TenantInput) error {
	var b strings.Builder
	b.WriteString("version: 1\ntenants:\n")
	for _, in := range inputs {
		fmt.Fprintf(&b, "  - key: %s\n    org_id: %s\n    salesforce:\n      instance_url: %s\n      auth: { type: oauth_client_credentials, client_id: %s, client_secret_ref: %q }\n    subscriptions: [ { topic: %s } ]\n",
			in.Key, in.OrgID, in.Salesforce.InstanceURL, in.Salesforce.Auth.ClientID, in.Salesforce.Auth.ClientSecretRef, in.Subscriptions[0].Topic)
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
