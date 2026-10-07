// Package schemareg is the Lakebase schema registry (sfzb.schemas): one row
// per (org, Avro schema ID) the service has decoded events with.
//
// Replicas consult it between their in-memory schema cache and Salesforce
// GetSchema, so a restart or a new replica does not refetch every org's
// schemas from Salesforce. Downstream jobs read it to generate typed views
// over the record VARIANT column. Rows are immutable: when a customer changes
// an object, Salesforce issues a new schema ID, which gets a new row.
package schemareg

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/obs"
)

// Record is one registered schema.
type Record struct {
	OrgID      string
	SchemaID   string
	TenantKey  string
	Topic      string
	EntityName string // derived from the schema's record name when empty
	JSON       string
}

const (
	defaultTimeout    = 5 * time.Second
	defaultRetryEvery = 30 * time.Second
	// maxPendingBytes bounds schemas held for writing while Lakebase is
	// unreachable; beyond it new schemas are dropped (and re-recorded the
	// next time a replica misses them).
	maxPendingBytes = 64 << 20
	maxBatchBytes   = 4 << 20
)

// Store reads and writes the registry table. Lookups are bounded by a
// timeout so callers can fall back to Salesforce; writes are queued and
// flushed in batches by Run, which retries failures, so a Lakebase outage
// never blocks ingestion.
type Store struct {
	pool       *pgxpool.Pool
	schema     string
	table      string // quoted schema.table
	logger     *slog.Logger
	timeout    time.Duration
	retryEvery time.Duration
	kick       chan struct{}

	mu           sync.Mutex
	pending      map[string]Record // keyed by org/schema ID
	pendingBytes int
}

// New wraps an open pool. schema is the Lakebase schema (default sfzb).
func New(pool *pgxpool.Pool, schema string, logger *slog.Logger) *Store {
	if schema == "" {
		schema = "sfzb"
	}
	return &Store{
		pool: pool, schema: schema, table: pgx.Identifier{schema, "schemas"}.Sanitize(), logger: logger,
		timeout: defaultTimeout, retryEvery: defaultRetryEvery, kick: make(chan struct{}, 1),
		pending: map[string]Record{},
	}
}

// Init creates the table if needed and verifies this role can read and
// write it.
func (s *Store) Init(ctx context.Context) error {
	stmts := []string{
		"CREATE SCHEMA IF NOT EXISTS " + pgx.Identifier{s.schema}.Sanitize(),
		`CREATE TABLE IF NOT EXISTS ` + s.table + ` (
  org_id        text        NOT NULL,
  schema_id     text        NOT NULL,
  tenant_key    text,
  topic         text,
  entity_name   text,
  schema_json   text        NOT NULL,
  first_seen_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (org_id, schema_id)
)`,
	}
	for _, q := range stmts {
		if _, err := s.pool.Exec(ctx, q); err != nil {
			return fmt.Errorf("schema registry init: %w", err)
		}
	}
	// CREATE INDEX needs table ownership even with IF NOT EXISTS, and an
	// operator may have created the table, so only create a missing index.
	index := pgx.Identifier{s.schema, "schemas_org_id_entity_name_idx"}.Sanitize()
	var hasIndex bool
	if err := s.pool.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", index).Scan(&hasIndex); err != nil {
		return fmt.Errorf("schema registry init: %w", err)
	}
	if !hasIndex {
		if _, err := s.pool.Exec(ctx, "CREATE INDEX IF NOT EXISTS schemas_org_id_entity_name_idx ON "+s.table+" (org_id, entity_name)"); err != nil {
			return fmt.Errorf("schema registry init: %w", err)
		}
	}
	var role string
	var canRead, canWrite bool
	err := s.pool.QueryRow(ctx, "SELECT current_user, has_table_privilege($1, 'SELECT'), has_table_privilege($1, 'INSERT')", s.table).
		Scan(&role, &canRead, &canWrite)
	if err != nil {
		return fmt.Errorf("schema registry init: %w", err)
	}
	if !canRead || !canWrite {
		return fmt.Errorf("schema registry %s is not readable and writable by %s; as the table owner run: GRANT SELECT, INSERT ON %s TO %s (or set SFZB_SCHEMA_REGISTRY=false)",
			s.table, role, s.table, pgx.Identifier{role}.Sanitize())
	}
	return nil
}

// GrantReaders lets roles (operators, the job that builds typed views)
// read the registry.
func (s *Store) GrantReaders(ctx context.Context, roles []string) error {
	for _, r := range roles {
		id := pgx.Identifier{r}.Sanitize()
		for _, q := range []string{
			"GRANT USAGE ON SCHEMA " + pgx.Identifier{s.schema}.Sanitize() + " TO " + id,
			"GRANT SELECT ON " + s.table + " TO " + id,
		} {
			if _, err := s.pool.Exec(ctx, q); err != nil {
				return fmt.Errorf("granting schema registry read to %s: %w", r, err)
			}
		}
	}
	return nil
}

// Get returns the schema JSON for (orgID, schemaID), if registered.
func (s *Store) Get(ctx context.Context, orgID, schemaID string) (string, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	var js string
	err := s.pool.QueryRow(ctx, "SELECT schema_json FROM "+s.table+" WHERE org_id = $1 AND schema_id = $2", orgID, schemaID).Scan(&js)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		obs.SchemaRegistryLookups.WithLabelValues("miss").Inc()
		return "", false, nil
	case err != nil:
		obs.SchemaRegistryLookups.WithLabelValues("error").Inc()
		return "", false, fmt.Errorf("schema registry lookup: %w", err)
	}
	obs.SchemaRegistryLookups.WithLabelValues("hit").Inc()
	return js, true, nil
}

// Put queues r to be recorded; Run writes it. It never blocks on Lakebase.
func (s *Store) Put(r Record) {
	if r.EntityName == "" {
		r.EntityName = EntityName(r.JSON)
	}
	key := r.OrgID + "/" + r.SchemaID
	s.mu.Lock()
	if _, ok := s.pending[key]; !ok {
		if s.pendingBytes+len(r.JSON) > maxPendingBytes {
			s.mu.Unlock()
			obs.SchemaRegistryWrites.WithLabelValues("dropped").Inc()
			return
		}
		s.pendingBytes += len(r.JSON)
		s.pending[key] = r
	}
	s.mu.Unlock()
	select {
	case s.kick <- struct{}{}:
	default:
	}
}

// Pending returns the number of schemas not yet written.
func (s *Store) Pending() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.pending)
}

// Run writes queued schemas until ctx is done. After a failed write it waits
// retryEvery before trying again.
func (s *Store) Run(ctx context.Context) {
	ticker := time.NewTicker(s.retryEvery)
	defer ticker.Stop()
	var retryAt time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.kick:
			if time.Now().Before(retryAt) {
				continue
			}
		case <-ticker.C:
		}
		if err := s.Flush(ctx); err != nil && ctx.Err() == nil {
			retryAt = time.Now().Add(s.retryEvery)
			s.logger.Warn("Recording schemas in the registry failed; will retry", "pending", s.Pending(), "retry_in", s.retryEvery, "error", err)
		}
	}
}

// Flush writes every queued schema, in batches.
func (s *Store) Flush(ctx context.Context) error {
	for {
		batch := s.nextBatch()
		if len(batch) == 0 {
			return nil
		}
		if err := s.insert(ctx, batch); err != nil {
			obs.SchemaRegistryWrites.WithLabelValues("error").Add(float64(len(batch)))
			return err
		}
		obs.SchemaRegistryWrites.WithLabelValues("ok").Add(float64(len(batch)))
		s.mu.Lock()
		for _, r := range batch {
			key := r.OrgID + "/" + r.SchemaID
			if _, ok := s.pending[key]; ok {
				delete(s.pending, key)
				s.pendingBytes -= len(r.JSON)
			}
		}
		s.mu.Unlock()
	}
}

func (s *Store) nextBatch() []Record {
	s.mu.Lock()
	defer s.mu.Unlock()
	var batch []Record
	size := 0
	for _, r := range s.pending {
		if len(batch) > 0 && size+len(r.JSON) > maxBatchBytes {
			break
		}
		batch = append(batch, r)
		size += len(r.JSON)
	}
	return batch
}

// insert writes rs; a schema already registered (by any replica) is kept.
func (s *Store) insert(ctx context.Context, rs []Record) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	n := len(rs)
	orgs, ids, tenants, topics, entities, docs := make([]string, n), make([]string, n), make([]*string, n), make([]*string, n), make([]*string, n), make([]string, n)
	for i, r := range rs {
		orgs[i], ids[i], docs[i] = r.OrgID, r.SchemaID, r.JSON
		tenants[i], topics[i], entities[i] = null(r.TenantKey), null(r.Topic), null(r.EntityName)
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO `+s.table+` (org_id, schema_id, tenant_key, topic, entity_name, schema_json)
SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[])
ON CONFLICT (org_id, schema_id) DO NOTHING`, orgs, ids, tenants, topics, entities, docs)
	if err != nil {
		return fmt.Errorf("recording %d schemas: %w", n, err)
	}
	return nil
}

// EntityName returns the object a Salesforce event schema describes, from
// its Avro record name: AccountChangeEvent is Account, Invoice__ChangeEvent
// is Invoice__c. Other events (such as platform events) keep their name.
func EntityName(schemaJSON string) string {
	var schema struct {
		Name string `json:"name"`
	}
	if json.Unmarshal([]byte(schemaJSON), &schema) != nil {
		return ""
	}
	name := schema.Name
	if i := strings.LastIndex(name, "."); i >= 0 {
		name = name[i+1:]
	}
	switch {
	case strings.HasSuffix(name, "__ChangeEvent"):
		return strings.TrimSuffix(name, "ChangeEvent") + "c"
	case strings.HasSuffix(name, "ChangeEvent") && name != "ChangeEvent":
		return strings.TrimSuffix(name, "ChangeEvent")
	}
	return name
}

func null(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
