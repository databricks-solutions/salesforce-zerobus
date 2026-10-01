// Package config loads service-level settings from flags and SFZB_*
// environment variables (flags win), and synthesizes a single tenant from the
// legacy SALESFORCE_* variables for drop-in migration.
package config

import (
	"errors"
	"flag"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/tenant"
)

// Service holds process-wide settings. Per-tenant settings live in the
// tenants YAML (see package tenant).
type Service struct {
	// TenantSource is lakebase (production registry tables), file (local
	// YAML, dev/test only), or env (single tenant from SALESFORCE_*).
	TenantSource        string
	TenantsFiles        []string // for TenantSource=file
	TenantsPollInterval time.Duration
	ShardIndex          string // integer or "auto"

	// Defaults for registry columns left NULL.
	DefaultTable           string
	DefaultReplay          string
	DefaultOnReplayExpired string
	DefaultBatchSize       int
	DefaultMaxUnacked      int
	DefaultAPIVersion      string
	ShardCount             int

	// Databricks service principal (also used for Zerobus OAuth).
	DatabricksHost string
	ClientID       string
	ClientSecret   string

	ZerobusEndpoint string
	UCEndpoint      string
	WarehouseID     string

	CheckpointStore    string // lakebase | delta | memory
	CheckpointInterval time.Duration
	LakebaseEndpoint   string // projects/<p>/branches/<b>/endpoints/<e>
	LakebaseHost       string
	LakebasePort       int
	LakebaseDatabase   string
	LakebaseUser       string
	LakebaseSchema     string
	LakebasePassRef    string // native Postgres password (secret ref) instead of OAuth
	LakebaseMaxConns   int
	LakebaseTokenTTL   time.Duration
	RegistryWriters    []string // Postgres roles granted registry write access

	Sink string // zerobus | memory

	SFPubSubAddr  string
	SFInsecure    bool
	SFSubsPerConn int

	ZBStreamsPerSDK    int
	ZBStreamsPerTable  string // "auto" or N
	ZBMaxBufferedBytes int64
	ZBMaxInflight      int
	ZBMaxPayloadBytes  int
	ZBBatchMaxRecords  int
	ZBBatchMaxBytes    int
	ZBBatchLinger      time.Duration
	ZBRecoveryRetries  int
	ZBRecoveryTimeout  time.Duration
	ZBRecoveryBackoff  time.Duration
	ZBLackOfAckTimeout time.Duration
	ZBFlushTimeout     time.Duration

	AuthRPS       float64
	SubscribeRPS  float64
	SchemaRPS     float64
	SecretsRPS    float64
	StartupSpread time.Duration
	SecretsTTL    time.Duration

	SchemaMode       string // migrate | verify | off
	SchemaCacheBytes int64

	AdminAddr       string
	EnablePprof     bool
	LogLevel        string
	LogFormat       string
	ShutdownTimeout time.Duration

	// Legacy is set when tenants came from SALESFORCE_* variables.
	Legacy       bool
	LegacyTenant LegacyTenant
}

// Getenv is os.Getenv; injected for tests.
type Getenv func(string) string

// Load parses flags (args excludes the program and subcommand name) with
// defaults from the environment.
func Load(args []string, getenv Getenv) (*Service, error) {
	s := &Service{}
	fs := flag.NewFlagSet("zerobus", flag.ContinueOnError)
	env := envReader{getenv: getenv}

	var tenantsFiles string
	fs.StringVar(&s.TenantSource, "tenant-source", env.str("SFZB_TENANT_SOURCE", ""), "lakebase | file | env (default: lakebase, or env when SALESFORCE_* is set)")
	fs.StringVar(&tenantsFiles, "tenants-file", env.str("SFZB_TENANTS_FILE", ""), "tenants YAML globs for -tenant-source=file (dev/test)")
	fs.DurationVar(&s.TenantsPollInterval, "tenants-poll-interval", env.dur("SFZB_TENANTS_POLL_INTERVAL", 30*time.Second), "registry poll interval")
	fs.StringVar(&s.DefaultTable, "default-table", env.str("SFZB_DEFAULT_TABLE", ""), "target table for subscriptions without one")
	fs.StringVar(&s.DefaultReplay, "default-replay", env.str("SFZB_DEFAULT_REPLAY", "LATEST"), "replay preset for subscriptions without a checkpoint")
	fs.StringVar(&s.DefaultOnReplayExpired, "default-on-replay-expired", env.str("SFZB_DEFAULT_ON_REPLAY_EXPIRED", "EARLIEST"), "preset used when a stored replay ID has expired")
	fs.IntVar(&s.DefaultBatchSize, "default-batch-size", env.int("SFZB_DEFAULT_BATCH_SIZE", 100), "events per FetchRequest (max 100)")
	fs.IntVar(&s.DefaultMaxUnacked, "default-max-unacked", env.int("SFZB_DEFAULT_MAX_UNACKED", 500), "per-subscription unacked event budget")
	fs.StringVar(&s.DefaultAPIVersion, "default-api-version", env.str("SFZB_DEFAULT_API_VERSION", "62.0"), "Salesforce API version for SOAP login")
	fs.StringVar(&s.ShardIndex, "shard-index", env.str("SFZB_SHARD_INDEX", "0"), `shard index, or "auto" (hostname ordinal)`)
	fs.IntVar(&s.ShardCount, "shard-count", env.int("SFZB_SHARD_COUNT", 1), "total shards (replicas)")

	fs.StringVar(&s.DatabricksHost, "databricks-host", env.str("DATABRICKS_HOST", env.str("DATABRICKS_WORKSPACE_URL", "")), "workspace URL")
	fs.StringVar(&s.ClientID, "databricks-client-id", env.str("DATABRICKS_CLIENT_ID", ""), "service principal application ID")
	s.ClientSecret = env.str("DATABRICKS_CLIENT_SECRET", "") // never a flag
	fs.StringVar(&s.ZerobusEndpoint, "zerobus-endpoint", env.str("SFZB_ZEROBUS_ENDPOINT", env.str("DATABRICKS_INGEST_ENDPOINT", "")), "Zerobus ingest endpoint")
	fs.StringVar(&s.UCEndpoint, "uc-endpoint", env.str("SFZB_UC_ENDPOINT", ""), "Unity Catalog endpoint (default: workspace URL)")
	fs.StringVar(&s.WarehouseID, "warehouse-id", env.str("SFZB_WAREHOUSE_ID", warehouseFromEndpoint(env.str("DATABRICKS_SQL_ENDPOINT", ""))), "SQL warehouse for table DDL")

	fs.StringVar(&s.CheckpointStore, "checkpoint-store", env.str("SFZB_CHECKPOINT_STORE", ""), "lakebase | delta | memory (default lakebase; delta in legacy mode without Lakebase)")
	fs.DurationVar(&s.CheckpointInterval, "checkpoint-interval", env.dur("SFZB_CHECKPOINT_INTERVAL", 5*time.Second), "checkpoint commit interval")
	fs.StringVar(&s.LakebaseEndpoint, "lakebase-endpoint", env.str("SFZB_LAKEBASE_ENDPOINT", ""), "projects/<p>/branches/<b>/endpoints/<e>")
	fs.StringVar(&s.LakebaseHost, "lakebase-host", env.str("SFZB_LAKEBASE_HOST", env.str("PGHOST", "")), "Postgres host (default: looked up from the endpoint)")
	fs.IntVar(&s.LakebasePort, "lakebase-port", env.int("SFZB_LAKEBASE_PORT", 5432), "Postgres port")
	fs.StringVar(&s.LakebaseDatabase, "lakebase-database", env.str("SFZB_LAKEBASE_DATABASE", env.str("PGDATABASE", "databricks_postgres")), "Postgres database")
	fs.StringVar(&s.LakebaseUser, "lakebase-user", env.str("SFZB_LAKEBASE_USER", env.str("PGUSER", "")), "Postgres role (default: service principal application ID)")
	fs.StringVar(&s.LakebaseSchema, "lakebase-schema", env.str("SFZB_LAKEBASE_SCHEMA", "sfzb"), "Postgres schema for checkpoints")
	fs.StringVar(&s.LakebasePassRef, "lakebase-password-ref", env.str("SFZB_LAKEBASE_PASSWORD_REF", ""), "secret ref for a native Postgres password (instead of OAuth)")
	fs.IntVar(&s.LakebaseMaxConns, "lakebase-max-conns", env.int("SFZB_LAKEBASE_MAX_CONNS", 4), "Postgres pool size")
	fs.DurationVar(&s.LakebaseTokenTTL, "lakebase-token-ttl", env.dur("SFZB_LAKEBASE_TOKEN_TTL", time.Hour), "OAuth database credential lifetime (5m-1h); refreshed at 75%")
	var writers string
	fs.StringVar(&writers, "registry-writers", env.str("SFZB_REGISTRY_WRITERS", ""), "comma-separated Postgres roles allowed to manage tenants")

	fs.StringVar(&s.Sink, "sink", env.str("SFZB_SINK", "zerobus"), "zerobus | memory (memory is for load tests)")

	fs.StringVar(&s.SFPubSubAddr, "sf-pubsub-addr", env.str("SFZB_SF_PUBSUB_ADDR", pubsubAddrFromLegacy(env)), "Salesforce Pub/Sub API host:port")
	fs.BoolVar(&s.SFInsecure, "sf-insecure", env.bool("SFZB_SF_INSECURE", false), "plaintext gRPC to Pub/Sub (tests only)")
	fs.IntVar(&s.SFSubsPerConn, "sf-subs-per-conn", env.int("SFZB_SF_SUBS_PER_CONN", 100), "Subscribe streams per gRPC connection")

	fs.IntVar(&s.ZBStreamsPerSDK, "zb-streams-per-sdk", env.int("SFZB_ZB_STREAMS_PER_SDK", 50), "Zerobus streams per SDK connection")
	fs.StringVar(&s.ZBStreamsPerTable, "zb-streams-per-table", env.str("SFZB_ZB_STREAMS_PER_TABLE", "auto"), `stream slots per table: "auto" or N`)
	fs.Int64Var(&s.ZBMaxBufferedBytes, "zb-max-buffered-bytes", env.int64("SFZB_ZB_MAX_BUFFERED_BYTES", 32<<20), "per-stream buffered payload cap")
	fs.IntVar(&s.ZBMaxInflight, "zb-max-inflight", env.int("SFZB_ZB_MAX_INFLIGHT", 50000), "per-stream unacked ingest calls")
	fs.IntVar(&s.ZBMaxPayloadBytes, "zb-max-payload-bytes", env.int("SFZB_ZB_MAX_PAYLOAD_BYTES", 8<<20), "max encoded row size (rows are truncated beyond this)")
	fs.IntVar(&s.ZBBatchMaxRecords, "zb-batch-max-records", env.int("SFZB_ZB_BATCH_MAX_RECORDS", 500), "records per Zerobus batch")
	fs.IntVar(&s.ZBBatchMaxBytes, "zb-batch-max-bytes", env.int("SFZB_ZB_BATCH_MAX_BYTES", 4<<20), "bytes per Zerobus batch")
	fs.DurationVar(&s.ZBBatchLinger, "zb-batch-linger", env.dur("SFZB_ZB_BATCH_LINGER", 20*time.Millisecond), "max wait to fill a batch")
	fs.IntVar(&s.ZBRecoveryRetries, "zb-recovery-retries", env.int("ZEROBUS_RECOVERY_RETRIES", 5), "SDK reconnect attempts")
	fs.DurationVar(&s.ZBRecoveryTimeout, "zb-recovery-timeout", env.durMs("ZEROBUS_RECOVERY_TIMEOUT_MS", 30*time.Second), "SDK per-attempt open timeout")
	fs.DurationVar(&s.ZBRecoveryBackoff, "zb-recovery-backoff", env.durMs("ZEROBUS_RECOVERY_BACKOFF_MS", 5*time.Second), "SDK delay between reconnects")
	fs.DurationVar(&s.ZBLackOfAckTimeout, "zb-lack-of-ack-timeout", env.durMs("ZEROBUS_SERVER_ACK_TIMEOUT_MS", 60*time.Second), "SDK ack silence before recovery")
	fs.DurationVar(&s.ZBFlushTimeout, "zb-flush-timeout", env.durMs("ZEROBUS_FLUSH_TIMEOUT_MS", 5*time.Minute), "SDK flush/close timeout")

	fs.Float64Var(&s.AuthRPS, "auth-rps", env.float("SFZB_AUTH_RPS", 20), "Salesforce logins per second")
	fs.Float64Var(&s.SubscribeRPS, "subscribe-rps", env.float("SFZB_SUBSCRIBE_RPS", 20), "Subscribe calls per second")
	fs.Float64Var(&s.SchemaRPS, "schema-rps", env.float("SFZB_SCHEMA_RPS", 20), "GetSchema calls per second")
	fs.Float64Var(&s.SecretsRPS, "secrets-rps", env.float("SFZB_SECRETS_RPS", 10), "Unity Catalog secret reads per second")
	fs.DurationVar(&s.StartupSpread, "startup-spread", env.dur("SFZB_STARTUP_SPREAD", 30*time.Second), "max random delay spreading first subscription starts (rate limits do the pacing)")
	fs.DurationVar(&s.SecretsTTL, "secrets-ttl", env.dur("SFZB_SECRETS_TTL", 15*time.Minute), "secret cache TTL")

	fs.StringVar(&s.SchemaMode, "schema-mode", env.str("SFZB_SCHEMA_MODE", "migrate"), "Delta table management: migrate | verify | off")
	fs.Int64Var(&s.SchemaCacheBytes, "schema-cache-bytes", env.int64("SFZB_SCHEMA_CACHE_BYTES", 256<<20), "Avro schema cache budget")

	fs.StringVar(&s.AdminAddr, "admin-addr", env.str("SFZB_ADMIN_ADDR", ":9090"), "admin HTTP listen address (empty disables)")
	fs.BoolVar(&s.EnablePprof, "pprof", env.bool("SFZB_ENABLE_PPROF", false), "serve /debug/pprof")
	fs.StringVar(&s.LogLevel, "log-level", env.str("SFZB_LOG_LEVEL", "info"), "debug | info | warn | error")
	fs.StringVar(&s.LogFormat, "log-format", env.str("SFZB_LOG_FORMAT", "json"), "json | text")
	fs.DurationVar(&s.ShutdownTimeout, "shutdown-timeout", env.dur("SFZB_SHUTDOWN_TIMEOUT", 45*time.Second), "graceful shutdown budget")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if len(env.errs) > 0 {
		return nil, errors.Join(env.errs...)
	}
	for _, w := range strings.Split(writers, ",") {
		if w = strings.TrimSpace(w); w != "" {
			s.RegistryWriters = append(s.RegistryWriters, w)
		}
	}
	for _, f := range strings.Split(tenantsFiles, ",") {
		if f = strings.TrimSpace(f); f != "" {
			s.TenantsFiles = append(s.TenantsFiles, f)
		}
	}
	if s.TenantSource == "" || s.TenantSource == "env" {
		lt, ok, err := LoadLegacyTenant(getenv)
		if err != nil {
			return nil, err
		}
		if ok {
			s.TenantSource, s.Legacy, s.LegacyTenant = "env", true, lt
		} else if s.TenantSource == "" {
			s.TenantSource = "lakebase"
		}
	}
	if s.TenantSource == "file" && len(s.TenantsFiles) == 0 {
		return nil, errors.New("SFZB_TENANTS_FILE is required with SFZB_TENANT_SOURCE=file")
	}
	if s.UCEndpoint == "" {
		s.UCEndpoint = s.DatabricksHost
	}
	if s.CheckpointStore == "" {
		s.CheckpointStore = "lakebase"
		if s.Legacy && s.LakebaseEndpoint == "" && s.LakebaseHost == "" {
			s.CheckpointStore = "delta"
		}
	}
	return s, nil
}

// Validate checks settings required to run the service.
func (s *Service) Validate() error {
	var errs []error
	req := func(name, v string) {
		if v == "" {
			errs = append(errs, fmt.Errorf("%s is required", name))
		}
	}
	switch s.TenantSource {
	case "lakebase", "file":
	case "env":
		if !s.Legacy {
			errs = append(errs, errors.New("SFZB_TENANT_SOURCE=env requires SALESFORCE_CHANGE_EVENT_CHANNEL and related variables"))
		}
	default:
		errs = append(errs, fmt.Errorf("SFZB_TENANT_SOURCE must be lakebase, file, or env, got %q", s.TenantSource))
	}
	if s.ShardCount < 1 {
		errs = append(errs, errors.New("SFZB_SHARD_COUNT must be >= 1"))
	}
	switch s.Sink {
	case "zerobus":
		req("DATABRICKS_HOST", s.DatabricksHost)
		req("DATABRICKS_CLIENT_ID", s.ClientID)
		req("DATABRICKS_CLIENT_SECRET", s.ClientSecret)
		req("SFZB_ZEROBUS_ENDPOINT", s.ZerobusEndpoint)
	case "memory":
	default:
		errs = append(errs, fmt.Errorf("SFZB_SINK must be zerobus or memory, got %q", s.Sink))
	}
	if s.CheckpointStore == "lakebase" || s.TenantSource == "lakebase" {
		if s.LakebaseEndpoint == "" && s.LakebaseHost == "" {
			errs = append(errs, errors.New("SFZB_LAKEBASE_ENDPOINT (or SFZB_LAKEBASE_HOST) is required for the lakebase checkpoint store"))
		}
		if s.LakebasePassRef == "" && s.LakebaseEndpoint == "" {
			errs = append(errs, errors.New("SFZB_LAKEBASE_ENDPOINT is required to mint OAuth credentials (or set SFZB_LAKEBASE_PASSWORD_REF)"))
		}
		if s.LakebasePassRef != "" && s.LakebaseUser == "" {
			errs = append(errs, errors.New("SFZB_LAKEBASE_USER is required with SFZB_LAKEBASE_PASSWORD_REF"))
		}
	}
	switch s.CheckpointStore {
	case "lakebase":
	case "delta":
		req("SFZB_WAREHOUSE_ID (or DATABRICKS_SQL_ENDPOINT)", s.WarehouseID)
	case "memory":
	default:
		errs = append(errs, fmt.Errorf("SFZB_CHECKPOINT_STORE must be lakebase, delta, or memory, got %q", s.CheckpointStore))
	}
	switch s.SchemaMode {
	case "migrate":
		if s.Sink == "zerobus" {
			req("SFZB_WAREHOUSE_ID (or DATABRICKS_SQL_ENDPOINT) for SFZB_SCHEMA_MODE=migrate", s.WarehouseID)
		}
	case "verify", "off":
	default:
		errs = append(errs, fmt.Errorf("SFZB_SCHEMA_MODE must be migrate, verify, or off, got %q", s.SchemaMode))
	}
	if s.LakebaseTokenTTL < 5*time.Minute || s.LakebaseTokenTTL > time.Hour {
		errs = append(errs, errors.New("SFZB_LAKEBASE_TOKEN_TTL must be between 5m and 1h"))
	}
	if s.ZBStreamsPerTable != "auto" {
		if n, err := strconv.Atoi(s.ZBStreamsPerTable); err != nil || n < 1 {
			errs = append(errs, fmt.Errorf("SFZB_ZB_STREAMS_PER_TABLE must be auto or a positive integer"))
		}
	}
	if s.ZBMaxPayloadBytes >= 9<<20 || s.ZBBatchMaxBytes > s.ZBMaxPayloadBytes {
		errs = append(errs, fmt.Errorf("SFZB_ZB_MAX_PAYLOAD_BYTES must be < 9MiB and SFZB_ZB_BATCH_MAX_BYTES must not exceed it"))
	}
	if s.SFSubsPerConn < 1 || s.ZBStreamsPerSDK < 1 {
		errs = append(errs, errors.New("pool sizes must be >= 1"))
	}
	return errors.Join(errs...)
}

// NeedsLakebase reports whether a Lakebase connection is required.
func (s *Service) NeedsLakebase() bool {
	return s.CheckpointStore == "lakebase" || s.TenantSource == "lakebase"
}

// Defaults returns the tenant defaults for registry rows.
func (s *Service) Defaults() tenant.Defaults {
	d := tenant.BuiltinDefaults()
	return d.Over(tenant.Defaults{
		Table:      s.DefaultTable,
		APIVersion: s.DefaultAPIVersion,
		Subscription: tenant.SubscriptionInput{
			ReplayDefault:   tenant.Preset(strings.ToUpper(s.DefaultReplay)),
			OnReplayExpired: tenant.Preset(strings.ToUpper(s.DefaultOnReplayExpired)),
			BatchSize:       s.DefaultBatchSize,
			MaxUnacked:      s.DefaultMaxUnacked,
		},
	})
}

// StreamsPerTable returns the stream slot count for a table with subs
// subscriptions.
func (s *Service) StreamsPerTable(subs int) int {
	if n, err := strconv.Atoi(s.ZBStreamsPerTable); err == nil && n > 0 {
		return n
	}
	n := (subs + 249) / 250
	return max(1, min(n, 16))
}

var warehousePattern = regexp.MustCompile(`/sql/1\.0/warehouses/([A-Za-z0-9]+)`)

func warehouseFromEndpoint(endpoint string) string {
	if m := warehousePattern.FindStringSubmatch(endpoint); m != nil {
		return m[1]
	}
	return ""
}

func pubsubAddrFromLegacy(env envReader) string {
	host := env.str("SALESFORCE_GRPC_HOST", "api.pubsub.salesforce.com")
	port := env.str("SALESFORCE_GRPC_PORT", "7443")
	return host + ":" + port
}

type envReader struct {
	getenv Getenv
	errs   []error
}

func (e *envReader) str(k, def string) string {
	if v := strings.TrimSpace(e.getenv(k)); v != "" {
		return v
	}
	return def
}

func (e *envReader) int(k string, def int) int {
	v := e.str(k, "")
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %w", k, err))
		return def
	}
	return n
}

func (e *envReader) int64(k string, def int64) int64 {
	v := e.str(k, "")
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %w", k, err))
		return def
	}
	return n
}

func (e *envReader) float(k string, def float64) float64 {
	v := e.str(k, "")
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %w", k, err))
		return def
	}
	return f
}

func (e *envReader) bool(k string, def bool) bool {
	v := e.str(k, "")
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %w", k, err))
		return def
	}
	return b
}

func (e *envReader) dur(k string, def time.Duration) time.Duration {
	v := e.str(k, "")
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %w", k, err))
		return def
	}
	return d
}

// durMs reads a legacy *_MS integer variable.
func (e *envReader) durMs(k string, def time.Duration) time.Duration {
	v := e.str(k, "")
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s: %w", k, err))
		return def
	}
	return time.Duration(n) * time.Millisecond
}
