// Package lakebase is the Lakebase Postgres checkpoint store.
//
// Authentication uses short-lived Databricks OAuth database credentials
// (minted via the Postgres API and injected on every new connection), or a
// native Postgres password for environments without OAuth.
package lakebase

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/databricks/databricks-sdk-go/service/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/backoff"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/checkpoint"
)

// CredentialAPI is the subset of the Databricks Postgres API used here.
type CredentialAPI interface {
	GenerateDatabaseCredential(ctx context.Context, req postgres.GenerateDatabaseCredentialRequest) (*postgres.DatabaseCredential, error)
	GetEndpoint(ctx context.Context, req postgres.GetEndpointRequest) (*postgres.Endpoint, error)
}

// Config describes the Lakebase connection.
type Config struct {
	Endpoint string // projects/<p>/branches/<b>/endpoints/<e>; used to mint tokens and look up Host
	Host     string
	Port     int
	Database string
	User     string // Postgres role; for a service principal, its application ID
	Schema   string
	MaxConns int
	// Password returns a native Postgres password. Nil means OAuth via API.
	Password func(ctx context.Context) (string, error)
	// ConnectTimeout bounds the initial connection (covers scale-to-zero wake).
	ConnectTimeout time.Duration
	// SSLMode defaults to "require" (Lakebase requires TLS).
	SSLMode string
}

// Connect opens a pgx pool to Lakebase and verifies connectivity.
func Connect(ctx context.Context, cfg Config, api CredentialAPI, logger *slog.Logger) (*pgxpool.Pool, error) {
	if cfg.Host == "" {
		if api == nil || cfg.Endpoint == "" {
			return nil, errors.New("lakebase: host or endpoint is required")
		}
		ep, err := api.GetEndpoint(ctx, postgres.GetEndpointRequest{Name: cfg.Endpoint})
		if err != nil {
			return nil, fmt.Errorf("lakebase: looking up endpoint %s: %w", cfg.Endpoint, err)
		}
		if ep.Status == nil || ep.Status.Hosts == nil || ep.Status.Hosts.Host == "" {
			return nil, fmt.Errorf("lakebase: endpoint %s has no host yet", cfg.Endpoint)
		}
		cfg.Host = ep.Status.Hosts.Host
	}
	if cfg.Port == 0 {
		cfg.Port = 5432
	}
	if cfg.MaxConns <= 0 {
		cfg.MaxConns = 4
	}
	if cfg.User == "" {
		return nil, errors.New("lakebase: user is required")
	}

	pw := cfg.Password
	if pw == nil {
		if api == nil || cfg.Endpoint == "" {
			return nil, errors.New("lakebase: endpoint is required for OAuth credentials")
		}
		tc := &tokenCache{api: api, endpoint: cfg.Endpoint, now: time.Now}
		pw = tc.token
	}

	if cfg.SSLMode == "" {
		cfg.SSLMode = "require"
	}
	connStr := fmt.Sprintf("host=%s port=%d dbname=%s user=%s sslmode=%s application_name=sfzb",
		cfg.Host, cfg.Port, quoteConnValue(cfg.Database), quoteConnValue(cfg.User), cfg.SSLMode)
	pc, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("lakebase: %w", err)
	}
	pc.MaxConns = int32(cfg.MaxConns)
	pc.MaxConnLifetime = 45 * time.Minute // OAuth tokens last 1h; Lakebase closes idle conns at 24h
	pc.MaxConnIdleTime = 10 * time.Minute
	pc.HealthCheckPeriod = time.Minute
	pc.BeforeConnect = func(ctx context.Context, cc *pgx.ConnConfig) error {
		p, err := pw(ctx)
		if err != nil {
			return fmt.Errorf("lakebase credential: %w", err)
		}
		cc.Password = p
		return nil
	}
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return nil, fmt.Errorf("lakebase: %w", err)
	}

	timeout := cfg.ConnectTimeout
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	pingCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	for attempt := 0; ; attempt++ {
		err = pool.Ping(pingCtx)
		if err == nil {
			return pool, nil
		}
		logger.Warn("Lakebase not reachable yet; retrying", "host", cfg.Host, "attempt", attempt+1, "error", err)
		if sleepErr := backoff.Sleep(pingCtx, attempt, 500*time.Millisecond, 10*time.Second); sleepErr != nil {
			pool.Close()
			return nil, fmt.Errorf("lakebase: connecting to %s: %w", cfg.Host, err)
		}
	}
}

// tokenCache mints and caches OAuth database credentials.
type tokenCache struct {
	api      CredentialAPI
	endpoint string
	now      func() time.Time

	mu      sync.Mutex
	cached  string
	expires time.Time
}

func (t *tokenCache) token(ctx context.Context) (string, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.cached != "" && t.now().Before(t.expires.Add(-15*time.Minute)) {
		return t.cached, nil
	}
	cred, err := t.api.GenerateDatabaseCredential(ctx, postgres.GenerateDatabaseCredentialRequest{Endpoint: t.endpoint})
	if err != nil {
		return "", err
	}
	t.cached = cred.Token
	t.expires = t.now().Add(time.Hour)
	if cred.ExpireTime != nil {
		t.expires = cred.ExpireTime.AsTime()
	}
	return t.cached, nil
}

// Store implements checkpoint.Store on a Lakebase table.
type Store struct {
	pool   *pgxpool.Pool
	schema string
	table  string // quoted schema.table
}

// New wraps an open pool. schema is created by Init if missing.
func New(pool *pgxpool.Pool, schema string) *Store {
	if schema == "" {
		schema = "sfzb"
	}
	return &Store{pool: pool, schema: schema, table: pgx.Identifier{schema, "checkpoints"}.Sanitize()}
}

// Init creates the schema and table. The service principal creates (and so
// owns) them; see `zerobus bootstrap lakebase` for the required grant.
func (s *Store) Init(ctx context.Context) error {
	stmts := []string{
		"CREATE SCHEMA IF NOT EXISTS " + pgx.Identifier{s.schema}.Sanitize(),
		`CREATE TABLE IF NOT EXISTS ` + s.table + ` (
  tenant_key    text        NOT NULL,
  topic         text        NOT NULL,
  org_id        text        NOT NULL,
  zerobus_table text        NOT NULL,
  replay_id     bytea       NOT NULL,
  events_acked  bigint      NOT NULL DEFAULT 0,
  owner         text,
  updated_at    timestamptz NOT NULL DEFAULT now(),
  created_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_key, topic)
)`,
	}
	for _, q := range stmts {
		if _, err := s.pool.Exec(ctx, q); err != nil {
			return fmt.Errorf("lakebase init: %w", err)
		}
	}
	return nil
}

// LoadMany loads checkpoints for keys with one query per 5000 tenants.
func (s *Store) LoadMany(ctx context.Context, keys []checkpoint.Key) (map[checkpoint.Key]checkpoint.Checkpoint, error) {
	want := map[checkpoint.Key]bool{}
	tenantSet := map[string]bool{}
	for _, k := range keys {
		want[k] = true
		tenantSet[k.Tenant] = true
	}
	tenants := make([]string, 0, len(tenantSet))
	for t := range tenantSet {
		tenants = append(tenants, t)
	}
	out := map[checkpoint.Key]checkpoint.Checkpoint{}
	const chunk = 5000
	for start := 0; start < len(tenants); start += chunk {
		part := tenants[start:min(start+chunk, len(tenants))]
		rows, err := s.pool.Query(ctx, `SELECT tenant_key, topic, org_id, zerobus_table, replay_id, events_acked, coalesce(owner, ''), updated_at
FROM `+s.table+` WHERE tenant_key = ANY($1)`, part)
		if err != nil {
			return nil, fmt.Errorf("loading checkpoints: %w", err)
		}
		for rows.Next() {
			var cp checkpoint.Checkpoint
			if err := rows.Scan(&cp.Key.Tenant, &cp.Key.Topic, &cp.OrgID, &cp.Table, &cp.ReplayID, &cp.EventsAcked, &cp.Owner, &cp.UpdatedAt); err != nil {
				rows.Close()
				return nil, fmt.Errorf("loading checkpoints: %w", err)
			}
			if want[cp.Key] {
				out[cp.Key] = cp
			}
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("loading checkpoints: %w", err)
		}
	}
	return out, nil
}

// SaveMany upserts checkpoints in one statement.
func (s *Store) SaveMany(ctx context.Context, cps []checkpoint.Checkpoint) error {
	if len(cps) == 0 {
		return nil
	}
	n := len(cps)
	tenants, topics, orgs, tables, owners := make([]string, n), make([]string, n), make([]string, n), make([]string, n), make([]string, n)
	replays := make([][]byte, n)
	acked := make([]int64, n)
	updated := make([]time.Time, n)
	for i, cp := range cps {
		tenants[i], topics[i], orgs[i], tables[i], owners[i] = cp.Key.Tenant, cp.Key.Topic, cp.OrgID, cp.Table, cp.Owner
		replays[i], acked[i], updated[i] = cp.ReplayID, cp.EventsAcked, cp.UpdatedAt
		if updated[i].IsZero() {
			updated[i] = time.Now()
		}
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO `+s.table+` AS c (tenant_key, topic, org_id, zerobus_table, replay_id, events_acked, owner, updated_at)
SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::bytea[], $6::bigint[], $7::text[], $8::timestamptz[])
ON CONFLICT (tenant_key, topic) DO UPDATE SET
  org_id = EXCLUDED.org_id,
  zerobus_table = EXCLUDED.zerobus_table,
  replay_id = EXCLUDED.replay_id,
  events_acked = c.events_acked + EXCLUDED.events_acked,
  owner = EXCLUDED.owner,
  updated_at = EXCLUDED.updated_at`,
		tenants, topics, orgs, tables, replays, acked, owners, updated)
	if err != nil {
		return fmt.Errorf("saving %d checkpoints: %w", n, err)
	}
	return nil
}

// Delete removes one checkpoint.
func (s *Store) Delete(ctx context.Context, k checkpoint.Key) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM `+s.table+` WHERE tenant_key = $1 AND topic = $2`, k.Tenant, k.Topic)
	return err
}

// Close closes the pool.
func (s *Store) Close() error {
	s.pool.Close()
	return nil
}

// Bootstrap grants a service principal what the service needs, connecting
// as a Lakebase superuser (the project owner deploying the bundle). It is
// idempotent. The service then creates and owns its schema on first start.
// writers are additional identities (users, groups, service principals)
// that get Postgres roles so the service can grant them registry access via
// SFZB_REGISTRY_WRITERS.
func Bootstrap(ctx context.Context, pool *pgxpool.Pool, database, spAppID, schema string, writers []string, logger *slog.Logger) error {
	if _, err := pool.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS databricks_auth"); err != nil {
		return fmt.Errorf("enabling databricks_auth: %w", err)
	}
	if err := ensureRole(ctx, pool, spAppID, "SERVICE_PRINCIPAL", logger); err != nil {
		return err
	}
	for _, w := range writers {
		if err := ensureRole(ctx, pool, w, identityType(w), logger); err != nil {
			return err
		}
	}
	role := pgx.Identifier{spAppID}.Sanitize()
	if _, err := pool.Exec(ctx, "GRANT CONNECT, CREATE ON DATABASE "+pgx.Identifier{database}.Sanitize()+" TO "+role); err != nil {
		return fmt.Errorf("granting database privileges: %w", err)
	}
	// If the schema already exists under another owner (e.g. created by a
	// human during local testing), grant the service principal full use.
	var owner string
	err := pool.QueryRow(ctx, "SELECT nspowner::regrole::text FROM pg_namespace WHERE nspname = $1", schema).Scan(&owner)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
	case err != nil:
		return fmt.Errorf("checking schema owner: %w", err)
	case strings.Trim(owner, `"`) != spAppID:
		s := pgx.Identifier{schema}.Sanitize()
		for _, q := range []string{
			"GRANT USAGE, CREATE ON SCHEMA " + s + " TO " + role,
			"GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA " + s + " TO " + role,
			"ALTER DEFAULT PRIVILEGES IN SCHEMA " + s + " GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO " + role,
		} {
			if _, err := pool.Exec(ctx, q); err != nil {
				return fmt.Errorf("granting schema privileges: %w", err)
			}
		}
		logger.Info("Granted existing schema to service principal", "schema", schema, "owner", owner)
	}
	return nil
}

func ensureRole(ctx context.Context, pool *pgxpool.Pool, name, identityType string, logger *slog.Logger) error {
	var exists bool
	if err := pool.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)", name).Scan(&exists); err != nil {
		return fmt.Errorf("checking role %s: %w", name, err)
	}
	if exists {
		return nil
	}
	if _, err := pool.Exec(ctx, "SELECT databricks_create_role($1, $2)", name, identityType); err != nil {
		return fmt.Errorf("creating Postgres role for %s: %w", name, err)
	}
	logger.Info("Created Postgres role", "role", name, "type", identityType)
	return nil
}

var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// identityType guesses the Databricks identity type from its name: emails
// are users, UUIDs are service principals, anything else is a group.
func identityType(name string) string {
	switch {
	case strings.Contains(name, "@"):
		return "USER"
	case uuidPattern.MatchString(name):
		return "SERVICE_PRINCIPAL"
	default:
		return "GROUP"
	}
}

func quoteConnValue(v string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(v, `\`, `\\`), `'`, `\'`) + "'"
}
