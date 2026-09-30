// Package delta derives checkpoints from the Delta event table itself: the
// latest row written for a subscription is by definition durable. It is
// read-only (progress is implicit in the table) and is used for single-tenant
// legacy mode and to seed Lakebase from rows written by the older services.
package delta

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/checkpoint"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/dbsql"
)

// Store implements checkpoint.Store over Delta event tables.
type Store struct {
	SQL *dbsql.Client
	// TableFor returns the target table of a checkpoint key.
	TableFor func(checkpoint.Key) (string, bool)
	// IncludeLegacyRows also matches rows without tenant_key/topic (written by
	// the Python service or go_salesforce_zerobus_cgo). Only safe when the
	// table holds a single org and topic.
	IncludeLegacyRows bool
}

func (s *Store) Init(context.Context) error { return nil }

func (s *Store) LoadMany(ctx context.Context, keys []checkpoint.Key) (map[checkpoint.Key]checkpoint.Checkpoint, error) {
	out := map[checkpoint.Key]checkpoint.Checkpoint{}
	for _, k := range keys {
		table, ok := s.TableFor(k)
		if !ok {
			continue
		}
		cp, found, err := s.load(ctx, k, table)
		if err != nil {
			return nil, err
		}
		if found {
			out[k] = cp
		}
	}
	return out, nil
}

func (s *Store) load(ctx context.Context, k checkpoint.Key, table string) (checkpoint.Checkpoint, bool, error) {
	qt, err := dbsql.QuoteTable(table)
	if err != nil {
		return checkpoint.Checkpoint{}, false, err
	}
	where := "tenant_key = :tenant AND topic = :topic"
	if s.IncludeLegacyRows {
		where = "((tenant_key = :tenant AND topic = :topic) OR (tenant_key IS NULL AND topic IS NULL))"
	}
	rows, err := s.SQL.Query(ctx,
		"SELECT org_id, replay_id FROM "+qt+" WHERE "+where+" AND replay_id IS NOT NULL ORDER BY processed_timestamp DESC LIMIT 1",
		dbsql.Param{Name: "tenant", Value: k.Tenant, Type: "STRING"},
		dbsql.Param{Name: "topic", Value: k.Topic, Type: "STRING"})
	if err != nil && s.IncludeLegacyRows && isMissingColumn(err) {
		// Table predates the multi-tenant columns (schema mode off/verify).
		rows, err = s.SQL.Query(ctx, "SELECT org_id, replay_id FROM "+qt+" WHERE replay_id IS NOT NULL ORDER BY timestamp DESC LIMIT 1")
	}
	if err != nil {
		if isMissingTable(err) {
			return checkpoint.Checkpoint{}, false, nil
		}
		return checkpoint.Checkpoint{}, false, fmt.Errorf("reading last replay ID from %s: %w", table, err)
	}
	if len(rows) == 0 || len(rows[0]) < 2 || rows[0][1] == "" {
		return checkpoint.Checkpoint{}, false, nil
	}
	replay, err := hex.DecodeString(rows[0][1])
	if err != nil {
		return checkpoint.Checkpoint{}, false, fmt.Errorf("replay_id %q in %s is not hex: %w", rows[0][1], table, err)
	}
	return checkpoint.Checkpoint{Key: k, OrgID: rows[0][0], Table: table, ReplayID: replay}, true, nil
}

// SaveMany is a no-op: acked rows are already in the table.
func (s *Store) SaveMany(context.Context, []checkpoint.Checkpoint) error { return nil }

func (s *Store) Delete(context.Context, checkpoint.Key) error {
	return errors.New("the delta checkpoint store is derived from the event table and cannot be reset; use -replay to override")
}

func (s *Store) Close() error { return nil }

func isMissingColumn(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "UNRESOLVED_COLUMN") || strings.Contains(msg, "cannot be resolved")
}

func isMissingTable(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "TABLE_OR_VIEW_NOT_FOUND")
}
