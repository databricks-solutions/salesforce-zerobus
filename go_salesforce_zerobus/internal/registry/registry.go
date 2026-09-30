// Package registry is the Lakebase-backed tenant registry.
//
// Operators onboard tenants by inserting rows (SQL, `zerobus tenants apply`,
// or any tool with write grants); the service polls a trigger-maintained
// version counter and reconciles subscriptions live. Invalid rows are skipped
// and reported in the subscription_status table instead of blocking the rest.
package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/tenant"
)

// Registry reads and writes the tenant tables.
type Registry struct {
	Pool     *pgxpool.Pool
	Schema   string
	Defaults tenant.Defaults
	Options  tenant.LoadOptions
	Interval time.Duration
	Logger   *slog.Logger
	// OnInvalid receives rows that failed validation on each load.
	OnInvalid func([]RowError)
}

// RowError is a tenant row that failed validation.
type RowError struct {
	Tenant string
	Topics []string
	Err    error
}

func (r *Registry) q(name string) string { return pgx.Identifier{r.schema(), name}.Sanitize() }

func (r *Registry) schema() string {
	if r.Schema == "" {
		return "sfzb"
	}
	return r.Schema
}

// Init creates the registry tables, triggers, and status table. Idempotent.
func (r *Registry) Init(ctx context.Context) error {
	s := pgx.Identifier{r.schema()}.Sanitize()
	stmts := []string{
		"CREATE SCHEMA IF NOT EXISTS " + s,
		`CREATE TABLE IF NOT EXISTS ` + r.q("tenants") + ` (
  tenant_key         text PRIMARY KEY CHECK (tenant_key ~ '^[a-z0-9][a-z0-9_-]{0,63}$'),
  enabled            boolean NOT NULL DEFAULT true,
  org_id             text CHECK (org_id IS NULL OR org_id ~ '^00D[A-Za-z0-9]{12}([A-Za-z0-9]{3})?$'),
  shard_pin          integer CHECK (shard_pin IS NULL OR shard_pin >= 0),
  labels             jsonb,
  instance_url       text NOT NULL CHECK (instance_url ~ '^https?://'),
  api_version        text,
  token_ttl_seconds  integer CHECK (token_ttl_seconds IS NULL OR token_ttl_seconds >= 300),
  auth_type          text NOT NULL CHECK (auth_type IN ('oauth_client_credentials', 'soap')),
  client_id          text,
  client_id_ref      text,
  client_secret_ref  text,
  username           text,
  username_ref       text,
  password_ref       text,
  security_token_ref text,
  created_at         timestamptz NOT NULL DEFAULT now(),
  updated_at         timestamptz NOT NULL DEFAULT now()
)`,
		`CREATE TABLE IF NOT EXISTS ` + r.q("subscriptions") + ` (
  tenant_key        text NOT NULL REFERENCES ` + r.q("tenants") + ` (tenant_key) ON DELETE CASCADE ON UPDATE CASCADE,
  topic             text NOT NULL CHECK (topic ~ '^/data/\w+$'),
  enabled           boolean NOT NULL DEFAULT true,
  target_table      text,
  replay_default    text CHECK (replay_default IS NULL OR replay_default IN ('LATEST', 'EARLIEST')),
  on_replay_expired text CHECK (on_replay_expired IS NULL OR on_replay_expired IN ('LATEST', 'EARLIEST')),
  batch_size        integer CHECK (batch_size IS NULL OR batch_size BETWEEN 1 AND 100),
  max_unacked       integer CHECK (max_unacked IS NULL OR max_unacked >= 1),
  dedup_size        integer CHECK (dedup_size IS NULL OR dedup_size >= 1),
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_key, topic)
)`,
		`CREATE TABLE IF NOT EXISTS ` + r.q("registry_version") + ` (
  id         integer PRIMARY KEY DEFAULT 1 CHECK (id = 1),
  version    bigint NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT now()
)`,
		`INSERT INTO ` + r.q("registry_version") + ` (id) VALUES (1) ON CONFLICT DO NOTHING`,
		`CREATE OR REPLACE FUNCTION ` + r.q("bump_registry_version") + `() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  UPDATE ` + r.q("registry_version") + ` SET version = version + 1, updated_at = now() WHERE id = 1;
  RETURN NULL;
END $$`,
		`CREATE OR REPLACE FUNCTION ` + r.q("touch_updated_at") + `() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END $$`,
		`CREATE TABLE IF NOT EXISTS ` + r.q("subscription_status") + ` (
  tenant_key    text NOT NULL,
  topic         text NOT NULL,
  state         text NOT NULL,
  detail        text,
  org_id        text,
  owner         text,
  restarts      integer NOT NULL DEFAULT 0,
  events_acked  bigint NOT NULL DEFAULT 0,
  last_event_at timestamptz,
  last_ack_at   timestamptz,
  updated_at    timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_key, topic)
)`,
	}
	for _, table := range []string{"tenants", "subscriptions"} {
		stmts = append(stmts,
			`CREATE OR REPLACE TRIGGER `+pgx.Identifier{table + "_version"}.Sanitize()+` AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON `+r.q(table)+
				` FOR EACH STATEMENT EXECUTE FUNCTION `+r.q("bump_registry_version")+`()`,
			`CREATE OR REPLACE TRIGGER `+pgx.Identifier{table + "_updated_at"}.Sanitize()+` BEFORE UPDATE ON `+r.q(table)+
				` FOR EACH ROW EXECUTE FUNCTION `+r.q("touch_updated_at")+`()`,
		)
	}
	for _, q := range stmts {
		if _, err := r.Pool.Exec(ctx, q); err != nil {
			return fmt.Errorf("registry init: %w", err)
		}
	}
	return nil
}

// GrantWriters lets existing Postgres roles manage tenants (read/write the
// registry tables, read status and checkpoints). The service owns the
// schema, so only it can grant; roles are created by `bootstrap lakebase`.
func (r *Registry) GrantWriters(ctx context.Context, roles []string) error {
	s := pgx.Identifier{r.schema()}.Sanitize()
	for _, role := range roles {
		id := pgx.Identifier{role}.Sanitize()
		for _, q := range []string{
			"GRANT USAGE ON SCHEMA " + s + " TO " + id,
			"GRANT SELECT, INSERT, UPDATE, DELETE ON " + r.q("tenants") + ", " + r.q("subscriptions") + " TO " + id,
			"GRANT SELECT ON " + r.q("subscription_status") + ", " + r.q("registry_version") + " TO " + id,
			"GRANT UPDATE ON " + r.q("registry_version") + " TO " + id, // bumped by triggers on writes
		} {
			if _, err := r.Pool.Exec(ctx, q); err != nil {
				return fmt.Errorf("granting registry access to %s: %w", role, err)
			}
		}
	}
	return nil
}

// Version returns the registry version counter.
func (r *Registry) Version(ctx context.Context) (int64, error) {
	var v int64
	err := r.Pool.QueryRow(ctx, "SELECT version FROM "+r.q("registry_version")+" WHERE id = 1").Scan(&v)
	return v, err
}

// Load reads a consistent snapshot of every tenant. Invalid tenants are
// excluded and returned as row errors.
func (r *Registry) Load(ctx context.Context) (tenant.Snapshot, []RowError, error) {
	tx, err := r.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return tenant.Snapshot{}, nil, fmt.Errorf("loading tenants: %w", err)
	}
	defer tx.Rollback(ctx)

	var version int64
	if err := tx.QueryRow(ctx, "SELECT version FROM "+r.q("registry_version")+" WHERE id = 1").Scan(&version); err != nil {
		return tenant.Snapshot{}, nil, fmt.Errorf("loading tenants: %w", err)
	}
	inputs, err := r.readInputs(ctx, tx)
	if err != nil {
		return tenant.Snapshot{}, nil, err
	}

	snap := tenant.Snapshot{Version: strconv.FormatInt(version, 10)}
	var rowErrs []RowError
	orgs := map[string]string{}
	for _, in := range inputs {
		t, err := tenant.Build(in, r.Defaults, r.q("tenants")+"["+in.Key+"]", r.Options)
		if err == nil && t.ExpectedOrgID != "" {
			if prev, dup := orgs[t.ExpectedOrgID]; dup {
				err = fmt.Errorf("org_id %s is also registered as tenant %q", t.ExpectedOrgID, prev)
			}
		}
		if err != nil {
			topics := make([]string, 0, len(in.Subscriptions))
			for _, s := range in.Subscriptions {
				topics = append(topics, s.Topic)
			}
			rowErrs = append(rowErrs, RowError{Tenant: in.Key, Topics: topics, Err: err})
			continue
		}
		if t.ExpectedOrgID != "" {
			orgs[t.ExpectedOrgID] = in.Key
		}
		snap.Tenants = append(snap.Tenants, t)
	}
	return snap, rowErrs, tx.Commit(ctx)
}

type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func (r *Registry) readInputs(ctx context.Context, q querier) ([]tenant.TenantInput, error) {
	rows, err := q.Query(ctx, `SELECT tenant_key, enabled, org_id, shard_pin, labels, instance_url, api_version, token_ttl_seconds,
  auth_type, client_id, client_id_ref, client_secret_ref, username, username_ref, password_ref, security_token_ref
FROM `+r.q("tenants")+` ORDER BY tenant_key`)
	if err != nil {
		return nil, fmt.Errorf("loading tenants: %w", err)
	}
	byKey := map[string]*tenant.TenantInput{}
	var order []string
	for rows.Next() {
		var (
			in                                 tenant.TenantInput
			enabled                            bool
			orgID, apiVersion                  *string
			shard, ttl                         *int32
			labels                             []byte
			clientID, clientIDRef, secretRef   *string
			username, usernameRef, passwordRef *string
			tokenRef                           *string
			authType                           string
		)
		if err := rows.Scan(&in.Key, &enabled, &orgID, &shard, &labels, &in.Salesforce.InstanceURL, &apiVersion, &ttl,
			&authType, &clientID, &clientIDRef, &secretRef, &username, &usernameRef, &passwordRef, &tokenRef); err != nil {
			rows.Close()
			return nil, fmt.Errorf("loading tenants: %w", err)
		}
		in.Enabled = &enabled
		in.OrgID = deref(orgID)
		if shard != nil {
			v := int(*shard)
			in.Shard = &v
		}
		if len(labels) > 0 {
			_ = json.Unmarshal(labels, &in.Labels) // non-string labels are ignored
		}
		in.Salesforce.APIVersion = deref(apiVersion)
		if ttl != nil {
			in.Salesforce.TokenTTL = time.Duration(*ttl) * time.Second
		}
		in.Salesforce.Auth = tenant.Auth{
			Type:             tenant.AuthType(authType),
			ClientID:         deref(clientID),
			ClientIDRef:      deref(clientIDRef),
			ClientSecretRef:  deref(secretRef),
			Username:         deref(username),
			UsernameRef:      deref(usernameRef),
			PasswordRef:      deref(passwordRef),
			SecurityTokenRef: deref(tokenRef),
		}
		byKey[in.Key] = &in
		order = append(order, in.Key)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("loading tenants: %w", err)
	}

	rows, err = q.Query(ctx, `SELECT tenant_key, topic, enabled, target_table, replay_default, on_replay_expired, batch_size, max_unacked, dedup_size
FROM `+r.q("subscriptions")+` ORDER BY tenant_key, topic`)
	if err != nil {
		return nil, fmt.Errorf("loading subscriptions: %w", err)
	}
	for rows.Next() {
		var (
			key, topic            string
			enabled               bool
			table, replay, expire *string
			batch, unacked, dedup *int32
		)
		if err := rows.Scan(&key, &topic, &enabled, &table, &replay, &expire, &batch, &unacked, &dedup); err != nil {
			rows.Close()
			return nil, fmt.Errorf("loading subscriptions: %w", err)
		}
		in, ok := byKey[key]
		if !ok {
			continue
		}
		in.Subscriptions = append(in.Subscriptions, tenant.SubscriptionInput{
			Topic: topic, Enabled: &enabled, Table: deref(table),
			ReplayDefault: tenant.Preset(deref(replay)), OnReplayExpired: tenant.Preset(deref(expire)),
			BatchSize: derefInt(batch), MaxUnacked: derefInt(unacked), DedupSize: derefInt(dedup),
		})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("loading subscriptions: %w", err)
	}
	out := make([]tenant.TenantInput, 0, len(order))
	for _, k := range order {
		out = append(out, *byKey[k])
	}
	return out, nil
}

// Watch implements tenant.Source: it loads the registry, then polls the
// version counter every Interval and emits a new snapshot on change. Poll
// failures (e.g. Lakebase briefly unavailable) keep the current snapshot.
func (r *Registry) Watch(ctx context.Context) (<-chan tenant.Snapshot, error) {
	snap, rowErrs, err := r.Load(ctx)
	if err != nil {
		return nil, err
	}
	r.reportInvalid(rowErrs)
	ch := make(chan tenant.Snapshot, 1)
	ch <- snap
	interval := r.Interval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		current := snap.Version
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}
			v, err := r.Version(ctx)
			if err != nil {
				if ctx.Err() == nil {
					r.Logger.Warn("Tenant registry poll failed; keeping current configuration", "error", err)
				}
				continue
			}
			if strconv.FormatInt(v, 10) == current {
				continue
			}
			next, rowErrs, err := r.Load(ctx)
			if err != nil {
				r.Logger.Warn("Tenant registry reload failed; keeping current configuration", "error", err)
				continue
			}
			r.reportInvalid(rowErrs)
			current = next.Version
			r.Logger.Info("Tenant registry changed", "version", next.Version, "tenants", len(next.Tenants), "invalid", len(rowErrs))
			select {
			case <-ch: // replace an unconsumed older snapshot
			default:
			}
			ch <- next
		}
	}()
	return ch, nil
}

func (r *Registry) reportInvalid(rowErrs []RowError) {
	for _, e := range rowErrs {
		r.Logger.Error("Skipping invalid tenant row", "tenant", e.Tenant, "error", e.Err)
	}
	if r.OnInvalid != nil {
		r.OnInvalid(rowErrs)
	}
}

// ApplyResult summarizes an Apply.
type ApplyResult struct {
	TenantsUpserted, SubscriptionsUpserted int
	TenantsDeleted, SubscriptionsDeleted   int64
}

// Apply upserts tenants from a YAML document in one transaction. The file's
// defaults block is written into each row; fields left unset stay NULL and
// take service defaults. With prune, tenants and subscriptions absent from
// the document are deleted. Every tenant is validated first; nothing is
// written if any is invalid.
func (r *Registry) Apply(ctx context.Context, doc tenant.Document, prune bool) (ApplyResult, error) {
	var res ApplyResult
	inputs := make([]tenant.TenantInput, len(doc.Tenants))
	var errs []error
	var built []tenant.Tenant
	for i, in := range doc.Tenants {
		inputs[i] = materialize(in, doc.Defaults)
		t, err := tenant.Build(inputs[i], r.Defaults, doc.Origins[i], r.Options)
		if err != nil {
			errs = append(errs, err)
		}
		built = append(built, t)
	}
	if err := tenant.ValidateSet(built); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return res, errors.Join(errs...)
	}

	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return res, err
	}
	defer tx.Rollback(ctx)
	keys := make([]string, 0, len(inputs))
	for _, in := range inputs {
		keys = append(keys, in.Key)
		if err := r.upsertTenant(ctx, tx, in); err != nil {
			return res, fmt.Errorf("tenant %s: %w", in.Key, err)
		}
		res.TenantsUpserted++
		topics := make([]string, 0, len(in.Subscriptions))
		for _, s := range in.Subscriptions {
			topics = append(topics, s.Topic)
			if err := r.upsertSubscription(ctx, tx, in.Key, s); err != nil {
				return res, fmt.Errorf("tenant %s subscription %s: %w", in.Key, s.Topic, err)
			}
			res.SubscriptionsUpserted++
		}
		if prune {
			tag, err := tx.Exec(ctx, "DELETE FROM "+r.q("subscriptions")+" WHERE tenant_key = $1 AND NOT (topic = ANY($2))", in.Key, topics)
			if err != nil {
				return res, err
			}
			res.SubscriptionsDeleted += tag.RowsAffected()
		}
	}
	if prune {
		tag, err := tx.Exec(ctx, "DELETE FROM "+r.q("tenants")+" WHERE NOT (tenant_key = ANY($1))", keys)
		if err != nil {
			return res, err
		}
		res.TenantsDeleted = tag.RowsAffected()
	}
	return res, tx.Commit(ctx)
}

func (r *Registry) upsertTenant(ctx context.Context, tx pgx.Tx, in tenant.TenantInput) error {
	enabled := in.Enabled == nil || *in.Enabled
	var labels []byte
	if len(in.Labels) > 0 {
		labels, _ = json.Marshal(in.Labels)
	}
	var ttl *int32
	if in.Salesforce.TokenTTL > 0 {
		v := int32(in.Salesforce.TokenTTL / time.Second)
		ttl = &v
	}
	var shard *int32
	if in.Shard != nil {
		v := int32(*in.Shard)
		shard = &v
	}
	a := in.Salesforce.Auth
	_, err := tx.Exec(ctx, `INSERT INTO `+r.q("tenants")+` (tenant_key, enabled, org_id, shard_pin, labels, instance_url, api_version,
  token_ttl_seconds, auth_type, client_id, client_id_ref, client_secret_ref, username, username_ref, password_ref, security_token_ref)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
ON CONFLICT (tenant_key) DO UPDATE SET enabled = EXCLUDED.enabled, org_id = EXCLUDED.org_id, shard_pin = EXCLUDED.shard_pin,
  labels = EXCLUDED.labels, instance_url = EXCLUDED.instance_url, api_version = EXCLUDED.api_version,
  token_ttl_seconds = EXCLUDED.token_ttl_seconds, auth_type = EXCLUDED.auth_type, client_id = EXCLUDED.client_id,
  client_id_ref = EXCLUDED.client_id_ref, client_secret_ref = EXCLUDED.client_secret_ref, username = EXCLUDED.username,
  username_ref = EXCLUDED.username_ref, password_ref = EXCLUDED.password_ref, security_token_ref = EXCLUDED.security_token_ref`,
		in.Key, enabled, null(in.OrgID), shard, labels, in.Salesforce.InstanceURL, null(in.Salesforce.APIVersion), ttl,
		string(a.Type), null(a.ClientID), null(a.ClientIDRef), null(a.ClientSecretRef), null(a.Username), null(a.UsernameRef),
		null(a.PasswordRef), null(a.SecurityTokenRef))
	return err
}

func (r *Registry) upsertSubscription(ctx context.Context, tx pgx.Tx, key string, s tenant.SubscriptionInput) error {
	enabled := s.Enabled == nil || *s.Enabled
	_, err := tx.Exec(ctx, `INSERT INTO `+r.q("subscriptions")+` (tenant_key, topic, enabled, target_table, replay_default, on_replay_expired,
  batch_size, max_unacked, dedup_size) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
ON CONFLICT (tenant_key, topic) DO UPDATE SET enabled = EXCLUDED.enabled, target_table = EXCLUDED.target_table,
  replay_default = EXCLUDED.replay_default, on_replay_expired = EXCLUDED.on_replay_expired, batch_size = EXCLUDED.batch_size,
  max_unacked = EXCLUDED.max_unacked, dedup_size = EXCLUDED.dedup_size`,
		key, s.Topic, enabled, null(s.Table), null(string(s.ReplayDefault)), null(string(s.OnReplayExpired)),
		nullInt(s.BatchSize), nullInt(s.MaxUnacked), nullInt(s.DedupSize))
	return err
}

// SetEnabled enables or disables a tenant (all of its subscriptions).
func (r *Registry) SetEnabled(ctx context.Context, key string, enabled bool) error {
	tag, err := r.Pool.Exec(ctx, "UPDATE "+r.q("tenants")+" SET enabled = $2 WHERE tenant_key = $1", key, enabled)
	if err == nil && tag.RowsAffected() == 0 {
		err = fmt.Errorf("tenant %q not found", key)
	}
	return err
}

// Status is one row of subscription_status.
type Status struct {
	Tenant, Topic, State, Detail, OrgID, Owner string
	Restarts                                   int
	EventsAcked                                int64
	LastEventAt, LastAckAt                     time.Time
}

// WriteStatus upserts subscription status rows in one statement.
func (r *Registry) WriteStatus(ctx context.Context, sts []Status) error {
	if len(sts) == 0 {
		return nil
	}
	sort.Slice(sts, func(i, j int) bool {
		if sts[i].Tenant != sts[j].Tenant {
			return sts[i].Tenant < sts[j].Tenant
		}
		return sts[i].Topic < sts[j].Topic
	})
	n := len(sts)
	tenants, topics, states, details, orgs, owners := make([]string, n), make([]string, n), make([]string, n), make([]*string, n), make([]*string, n), make([]*string, n)
	restarts := make([]int32, n)
	acked := make([]int64, n)
	lastEvent, lastAck := make([]*time.Time, n), make([]*time.Time, n)
	for i, s := range sts {
		tenants[i], topics[i], states[i] = s.Tenant, s.Topic, s.State
		details[i], orgs[i], owners[i] = null(truncate(s.Detail, 2000)), null(s.OrgID), null(s.Owner)
		restarts[i], acked[i] = int32(s.Restarts), s.EventsAcked
		lastEvent[i], lastAck[i] = nullTime(s.LastEventAt), nullTime(s.LastAckAt)
	}
	_, err := r.Pool.Exec(ctx, `INSERT INTO `+r.q("subscription_status")+` AS s (tenant_key, topic, state, detail, org_id, owner, restarts,
  events_acked, last_event_at, last_ack_at, updated_at)
SELECT *, now() FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::int[], $8::bigint[],
  $9::timestamptz[], $10::timestamptz[])
ON CONFLICT (tenant_key, topic) DO UPDATE SET state = EXCLUDED.state, detail = EXCLUDED.detail,
  org_id = coalesce(EXCLUDED.org_id, s.org_id), owner = EXCLUDED.owner, restarts = EXCLUDED.restarts,
  events_acked = EXCLUDED.events_acked, last_event_at = coalesce(EXCLUDED.last_event_at, s.last_event_at),
  last_ack_at = coalesce(EXCLUDED.last_ack_at, s.last_ack_at), updated_at = now()`,
		tenants, topics, states, details, orgs, owners, restarts, acked, lastEvent, lastAck)
	return err
}

// PruneStatus deletes status rows for subscriptions no longer registered.
func (r *Registry) PruneStatus(ctx context.Context) error {
	_, err := r.Pool.Exec(ctx, `DELETE FROM `+r.q("subscription_status")+` st WHERE NOT EXISTS (
  SELECT 1 FROM `+r.q("subscriptions")+` s WHERE s.tenant_key = st.tenant_key AND s.topic = st.topic)`)
	return err
}

// materialize writes a YAML file's defaults block into a tenant so the
// stored rows do not depend on the file.
func materialize(in tenant.TenantInput, d tenant.Defaults) tenant.TenantInput {
	if in.Salesforce.APIVersion == "" {
		in.Salesforce.APIVersion = d.APIVersion
	}
	if in.Salesforce.TokenTTL == 0 {
		in.Salesforce.TokenTTL = d.TokenTTL
	}
	subs := make([]tenant.SubscriptionInput, len(in.Subscriptions))
	for i, s := range in.Subscriptions {
		if s.Table == "" {
			s.Table = firstNonEmpty(d.Subscription.Table, d.Table)
		}
		if s.ReplayDefault == "" {
			s.ReplayDefault = d.Subscription.ReplayDefault
		}
		if s.OnReplayExpired == "" {
			s.OnReplayExpired = d.Subscription.OnReplayExpired
		}
		if s.BatchSize == 0 {
			s.BatchSize = d.Subscription.BatchSize
		}
		if s.MaxUnacked == 0 {
			s.MaxUnacked = d.Subscription.MaxUnacked
		}
		if s.DedupSize == 0 {
			s.DedupSize = d.Subscription.DedupSize
		}
		subs[i] = s
	}
	in.Subscriptions = subs
	return in
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func derefInt(v *int32) int {
	if v == nil {
		return 0
	}
	return int(*v)
}

func null(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func nullInt(v int) *int32 {
	if v == 0 {
		return nil
	}
	n := int32(v)
	return &n
}

func nullTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
