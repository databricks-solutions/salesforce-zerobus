// Package app wires the service together and owns its lifecycle.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"sync/atomic"
	"time"

	"github.com/databricks/databricks-sdk-go"
	zb "github.com/databricks/zerobus-sdk/purego/zerobus"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/checkpoint"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/checkpoint/delta"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/checkpoint/lakebase"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/config"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/dbsql"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/obs"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/pubsub"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/registry"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/secrets"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/sfauth"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/shard"
	zbsink "github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/sink/zerobus"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/supervisor"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/tablemgr"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/tenant"
)

// Options configures Run. Fields other than Config and Logger are overrides
// used by tests and the load generator.
type Options struct {
	Config  *config.Service
	Logger  *slog.Logger
	Version string

	Factory zbsink.Factory    // default: Zerobus SDK (or memory when SFZB_SINK=memory)
	Store   checkpoint.Store  // default: from SFZB_CHECKPOINT_STORE
	Source  tenant.Source     // default: from SFZB_TENANT_SOURCE
	Ready   func(addr string) // called with the admin address once serving
	Policy  *supervisor.Policy
}

// Env holds shared clients built from configuration.
type Env struct {
	Config    *config.Service
	Logger    *slog.Logger
	Workspace *databricks.WorkspaceClient
	Secrets   *secrets.Resolver
	Pool      *pgxpool.Pool
}

// NewEnv builds the workspace client, secret resolver, and (if needed) the
// Lakebase pool.
func NewEnv(ctx context.Context, cfg *config.Service, logger *slog.Logger, needLakebase bool) (*Env, error) {
	e := &Env{Config: cfg, Logger: logger}
	if cfg.DatabricksHost != "" {
		w, err := databricks.NewWorkspaceClient(&databricks.Config{Host: cfg.DatabricksHost, ClientID: cfg.ClientID, ClientSecret: cfg.ClientSecret})
		if err != nil {
			return nil, fmt.Errorf("databricks client: %w", err)
		}
		e.Workspace = w
	}
	providers := []secrets.Provider{secrets.Env{}, secrets.File{}}
	if e.Workspace != nil {
		providers = append(providers, secrets.UnityCatalog{API: e.Workspace.SecretsUc, Limiter: limiter(cfg.SecretsRPS)})
	} else {
		providers = append(providers, secrets.UnityCatalog{})
	}
	e.Secrets = secrets.NewResolver(cfg.SecretsTTL, providers...)
	if needLakebase {
		pool, err := e.ConnectLakebase(ctx, cfg.LakebaseUser)
		if err != nil {
			return nil, err
		}
		e.Pool = pool
	}
	return e, nil
}

// ConnectLakebase opens a pool as user (default: the service principal).
func (e *Env) ConnectLakebase(ctx context.Context, user string) (*pgxpool.Pool, error) {
	cfg := e.Config
	if user == "" {
		user = cfg.ClientID
	}
	lc := lakebase.Config{
		Endpoint: cfg.LakebaseEndpoint, Host: cfg.LakebaseHost, Port: cfg.LakebasePort, Database: cfg.LakebaseDatabase,
		User: user, Schema: cfg.LakebaseSchema, MaxConns: cfg.LakebaseMaxConns,
	}
	if ref := cfg.LakebasePassRef; ref != "" {
		lc.Password = func(ctx context.Context) (string, error) { return e.Secrets.Resolve(ctx, ref) }
	}
	var api lakebase.CredentialAPI
	if e.Workspace != nil {
		api = e.Workspace.Postgres
	}
	return lakebase.Connect(ctx, lc, api, e.Logger)
}

// Registry returns the tenant registry over the Lakebase pool.
func (e *Env) Registry(shardCount int) *registry.Registry {
	return &registry.Registry{
		Pool: e.Pool, Schema: e.Config.LakebaseSchema, Defaults: e.Config.Defaults(),
		Options:  tenant.LoadOptions{SecretSchemes: e.Secrets.Schemes(), ShardCount: shardCount},
		Interval: e.Config.TenantsPollInterval, Logger: e.Logger,
	}
}

// SQL returns the SQL warehouse client.
func (e *Env) SQL() *dbsql.Client {
	c := &dbsql.Client{WarehouseID: e.Config.WarehouseID}
	if e.Workspace != nil {
		c.API = e.Workspace.StatementExecution
	}
	return c
}

// TableManager returns the Delta table manager, or nil when disabled.
func (e *Env) TableManager() *tablemgr.Manager {
	if e.Config.SchemaMode == "off" || e.Workspace == nil {
		return nil
	}
	return &tablemgr.Manager{Tables: e.Workspace.Tables, SQL: e.SQL(), Mode: tablemgr.Mode(e.Config.SchemaMode), Logger: e.Logger}
}

// Close releases the pool.
func (e *Env) Close() {
	if e.Pool != nil {
		e.Pool.Close()
	}
}

// Run runs the service until ctx is cancelled, then shuts down gracefully.
func Run(ctx context.Context, opts Options) error {
	cfg, logger := opts.Config, opts.Logger
	index, err := shard.ResolveIndex(cfg.ShardIndex)
	if err != nil {
		return err
	}
	assigner, err := shard.NewStaticHash(index, cfg.ShardCount)
	if err != nil {
		return err
	}
	obs.ShardInfo.WithLabelValues(fmt.Sprint(index), fmt.Sprint(cfg.ShardCount)).Set(1)
	owner, _ := os.Hostname()
	logger = logger.With("shard", index)
	logger.Info("Starting salesforce-zerobus", "version", opts.Version, "shard", index, "shard_count", cfg.ShardCount,
		"tenant_source", cfg.TenantSource, "checkpoint_store", cfg.CheckpointStore, "sink", cfg.Sink)

	needLakebase := cfg.NeedsLakebase() && (opts.Store == nil || opts.Source == nil)
	env, err := NewEnv(ctx, cfg, logger, needLakebase)
	if err != nil {
		return err
	}
	defer env.Close()

	// Latest owned snapshot, for components that need spec lookups.
	var current atomic.Pointer[tenant.Snapshot]

	// Tenant registry.
	var reg *registry.Registry
	source := opts.Source
	if source == nil {
		switch cfg.TenantSource {
		case "lakebase":
			reg = env.Registry(cfg.ShardCount)
			if err := reg.Init(ctx); err != nil {
				return err
			}
			if err := reg.GrantWriters(ctx, cfg.RegistryWriters); err != nil {
				return err
			}
			source = reg
		case "file":
			snap, err := tenant.LoadFiles(cfg.TenantsFiles, tenant.LoadOptions{SecretSchemes: env.Secrets.Schemes(), ShardCount: cfg.ShardCount}, cfg.Defaults())
			if err != nil {
				return err
			}
			source = tenant.StaticSource{Snapshot: snap}
		case "env":
			source = tenant.StaticSource{Snapshot: tenant.Snapshot{Version: "env", Tenants: []tenant.Tenant{cfg.LegacyTenant.Tenant}}}
		}
	}

	// Checkpoint store.
	store := opts.Store
	if store == nil {
		tableFor := func(k checkpoint.Key) (string, bool) {
			snap := current.Load()
			if snap == nil {
				return "", false
			}
			t, ok := snap.Tenant(tenant.Key(k.Tenant))
			if !ok {
				return "", false
			}
			for _, s := range t.Subscriptions {
				if s.Key.Topic == k.Topic {
					return s.Table, true
				}
			}
			return "", false
		}
		deltaStore := &delta.Store{SQL: env.SQL(), TableFor: tableFor, IncludeLegacyRows: cfg.Legacy}
		switch cfg.CheckpointStore {
		case "lakebase":
			lb := lakebase.New(env.Pool, cfg.LakebaseSchema)
			if err := lb.Init(ctx); err != nil {
				return err
			}
			store = lb
			if cfg.Legacy && cfg.LegacyTenant.SeedFromDelta && cfg.WarehouseID != "" {
				store = checkpoint.WithFallback(lb, deltaStore)
			}
		case "delta":
			store = deltaStore
		default:
			store = checkpoint.NewMemoryStore()
		}
	}
	defer store.Close()

	// Sink.
	factory := opts.Factory
	if factory == nil {
		if cfg.Sink == "memory" {
			mf := zbsink.NewMemoryFactory(0)
			mf.KeepRows = false
			factory = mf
		} else {
			factory, err = zbsink.NewSDKFactory(zbsink.SDKConfig{
				ZerobusEndpoint: cfg.ZerobusEndpoint, UCEndpoint: cfg.UCEndpoint, ClientID: cfg.ClientID, ClientSecret: cfg.ClientSecret,
				AppName: "salesforce-zerobus/" + opts.Version, StreamsPerSDK: cfg.ZBStreamsPerSDK,
				StreamOptions: []zb.StreamOption{
					zb.WithMaxInflight(cfg.ZBMaxInflight),
					zb.WithMaxBufferedPayloadBytes(cfg.ZBMaxBufferedBytes),
					zb.WithRecoveryRetries(cfg.ZBRecoveryRetries),
					zb.WithRecoveryTimeout(cfg.ZBRecoveryTimeout),
					zb.WithRecoveryBackoff(cfg.ZBRecoveryBackoff),
					zb.WithLackOfAckTimeout(cfg.ZBLackOfAckTimeout),
					zb.WithFlushTimeout(cfg.ZBFlushTimeout),
				},
			})
			if err != nil {
				return err
			}
		}
	}
	sinkOpts := zbsink.Options{
		SlotsForTable: cfg.StreamsPerTable, BatchMaxRecords: cfg.ZBBatchMaxRecords, BatchMaxBytes: cfg.ZBBatchMaxBytes,
		BatchLinger: cfg.ZBBatchLinger, Logger: logger,
	}
	if cfg.Sink == "zerobus" && opts.Factory == nil {
		if tm := env.TableManager(); tm != nil {
			sinkOpts.EnsureTable = tm.Ensure
		}
	}
	snk := zbsink.New(factory, sinkOpts)

	// Salesforce side.
	conns := pubsub.NewConnPool(cfg.SFPubSubAddr, cfg.SFSubsPerConn, cfg.SFInsecure)
	defer conns.Close()
	shared := &pubsub.Shared{
		Conns: conns, Schemas: pubsub.NewSchemaCache(cfg.SchemaCacheBytes),
		SubscribeLimiter: limiter(cfg.SubscribeRPS), SchemaLimiter: limiter(cfg.SchemaRPS),
		MaxRowBytes: cfg.ZBMaxPayloadBytes, Logger: logger,
	}
	wms := checkpoint.NewRegistry()
	sup := supervisor.New(supervisor.Deps{
		Shared: shared, Sink: snk, Registry: wms, Store: store, HTTP: sfauth.NewHTTPClient(),
		Resolve: env.Secrets.Resolve, AuthLimiter: limiter(cfg.AuthRPS), StartupSpread: cfg.StartupSpread, Logger: logger,
		Policy: opts.Policy,
	})
	committer := &checkpoint.Committer{Registry: wms, Store: store, Interval: cfg.CheckpointInterval, Owner: owner, Logger: logger}

	if reg != nil && index == 0 {
		// One replica reports rows that failed validation.
		reg.OnInvalid = func(rows []registry.RowError) {
			var sts []registry.Status
			for _, r := range rows {
				topics := r.Topics
				if len(topics) == 0 {
					topics = []string{"*"}
				}
				for _, t := range topics {
					sts = append(sts, registry.Status{Tenant: r.Tenant, Topic: t, State: "invalid_config", Detail: r.Err.Error(), Owner: owner})
				}
			}
			if err := reg.WriteStatus(context.Background(), sts); err != nil {
				logger.Warn("Writing invalid-row status failed", "error", err)
			}
		}
	}

	// Watch tenants and filter to this shard.
	runCtx, stopRunning := context.WithCancel(ctx)
	defer stopRunning()
	raw, err := source.Watch(runCtx)
	if err != nil {
		return fmt.Errorf("loading tenants: %w", err)
	}
	owned := make(chan tenant.Snapshot, 1)
	go func() {
		for {
			select {
			case <-runCtx.Done():
				return
			case snap := <-raw:
				mine, err := assigner.Filter(runCtx, snap)
				if err != nil {
					logger.Error("Shard filter failed", "error", err)
					continue
				}
				current.Store(&mine)
				select {
				case <-owned:
				default:
				}
				owned <- mine
			}
		}
	}()

	admin := &obs.Admin{
		Addr: cfg.AdminAddr, Pprof: cfg.EnablePprof, Logger: logger,
		Live: func() error {
			if age := time.Since(sup.Heartbeat()); age > time.Minute {
				return fmt.Errorf("supervisor stalled for %s", age.Round(time.Second))
			}
			return nil
		},
		Ready: func() error {
			if !sup.Reconciled() {
				return errors.New("tenants not loaded yet")
			}
			if !snk.Ready() {
				return errors.New("zerobus streams opening")
			}
			return nil
		},
		Subscriptions: func(state, tenantKey, table string) any {
			var out []supervisor.SubStatus
			for _, s := range sup.Statuses() {
				if (state == "" || s.State == state) && (tenantKey == "" || s.Tenant == tenantKey) && (table == "" || s.Table == table) {
					out = append(out, s)
				}
			}
			return out
		},
		Streams: func() any { return snk.Health() },
	}

	g, gctx := errgroup.WithContext(runCtx)
	adminCtx, stopAdmin := context.WithCancel(context.Background())
	defer stopAdmin()
	adminDone := make(chan error, 1)
	if cfg.AdminAddr != "" {
		addr, err := admin.Listen()
		if err != nil {
			return fmt.Errorf("admin server: %w", err)
		}
		go func() { adminDone <- admin.Serve(adminCtx) }()
		if opts.Ready != nil {
			opts.Ready(addr)
		}
	} else {
		adminDone <- nil
	}
	g.Go(func() error { return sup.Run(gctx, owned) })
	g.Go(func() error { return committer.Run(gctx) })
	g.Go(func() error { statusLoop(gctx, sup, reg, owner, index, logger); return nil })

	<-gctx.Done()
	logger.Info("Shutting down", "reason", context.Cause(gctx))
	stopRunning()
	runErr := g.Wait() // supervisor has stopped every subscription

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := snk.Close(shutdownCtx); err != nil {
		logger.Warn("Sink close incomplete", "error", err)
	}
	if err := committer.Flush(shutdownCtx); err != nil {
		logger.Error("Final checkpoint commit failed; up to one interval of events will be redelivered", "error", err)
	}
	if reg != nil {
		writeStatus(shutdownCtx, sup.FinalStatuses(), reg, owner, "stopped")
	}
	stopAdmin()
	<-adminDone
	logger.Info("salesforce-zerobus stopped")
	if ctx.Err() != nil {
		return nil
	}
	return runErr
}

// statusLoop writes subscription status to the registry and logs a summary.
func statusLoop(ctx context.Context, sup *supervisor.Supervisor, reg *registry.Registry, owner string, index int, logger *slog.Logger) {
	write := time.NewTicker(30 * time.Second)
	summary := time.NewTicker(time.Minute)
	prune := time.NewTicker(10 * time.Minute)
	defer write.Stop()
	defer summary.Stop()
	defer prune.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-write.C:
			if reg != nil {
				writeStatus(ctx, sup.Statuses(), reg, owner, "")
			}
		case <-prune.C:
			if reg != nil && index == 0 {
				if err := reg.PruneStatus(ctx); err != nil {
					logger.Warn("Pruning status rows failed", "error", err)
				}
			}
		case <-summary.C:
			logSummary(sup.Statuses(), logger)
		}
	}
}

func writeStatus(ctx context.Context, statuses []supervisor.SubStatus, reg *registry.Registry, owner, override string) {
	var sts []registry.Status
	for _, s := range statuses {
		state, detail := s.State, s.LastError
		if override != "" {
			state = override
		}
		if state == supervisor.StateRunning {
			detail = ""
		}
		sts = append(sts, registry.Status{
			Tenant: s.Tenant, Topic: s.Topic, State: state, Detail: detail, OrgID: s.OrgID, Owner: owner,
			Restarts: s.Restarts, EventsAcked: s.Acked, LastEventAt: s.LastEventAt, LastAckAt: s.LastAckAt,
		})
	}
	if err := reg.WriteStatus(ctx, sts); err != nil && ctx.Err() == nil {
		reg.Logger.Warn("Writing subscription status failed", "error", err)
	}
}

func logSummary(sts []supervisor.SubStatus, logger *slog.Logger) {
	counts := map[string]int{}
	var failing []supervisor.SubStatus
	for _, s := range sts {
		counts[s.State]++
		if s.State != supervisor.StateRunning && s.LastError != "" {
			failing = append(failing, s)
		}
	}
	sort.Slice(failing, func(i, j int) bool { return failing[i].Restarts > failing[j].Restarts })
	attrs := []any{"subscriptions", len(sts), "by_state", counts}
	for i, s := range failing[:min(10, len(failing))] {
		attrs = append(attrs, fmt.Sprintf("top%d", i+1), fmt.Sprintf("%s %s [%s x%d] %s", s.Tenant, s.Topic, s.LastErrorClass, s.Restarts, truncate(s.LastError, 160)))
	}
	logger.Info("Subscription summary", attrs...)
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

func limiter(rps float64) *rate.Limiter {
	if rps <= 0 {
		return nil
	}
	return rate.NewLimiter(rate.Limit(rps), max(1, int(rps)))
}
