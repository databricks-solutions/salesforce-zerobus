// Package tablemgr creates and additively migrates target Delta tables so
// their columns match the SalesforceEvent protobuf. Zerobus rejects streams
// whose descriptor has fields the table lacks, so this runs before a table's
// first stream opens. Columns are only ever added, never dropped or retyped.
package tablemgr

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/databricks/databricks-sdk-go/apierr"
	"github.com/databricks/databricks-sdk-go/service/catalog"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/dbsql"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/proto/eventpb"
)

// Mode selects how tables are managed.
type Mode string

const (
	ModeMigrate Mode = "migrate" // create missing tables, add missing columns
	ModeVerify  Mode = "verify"  // fail if the table or columns are missing
	ModeOff     Mode = "off"
)

// TablesAPI is the subset of the Unity Catalog Tables API used here.
type TablesAPI interface {
	Get(ctx context.Context, req catalog.GetTableRequest) (*catalog.TableInfo, error)
}

// Column is a Delta column derived from the protobuf.
type Column struct{ Name, Type string }

// Columns returns the expected columns in declaration order.
func Columns() []Column {
	fields := (&eventpb.SalesforceEvent{}).ProtoReflect().Descriptor().Fields()
	out := make([]Column, 0, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)
		out = append(out, Column{Name: string(f.Name()), Type: deltaType(f)})
	}
	return out
}

func deltaType(f protoreflect.FieldDescriptor) string {
	var t string
	switch f.Kind() {
	case protoreflect.StringKind:
		t = "STRING"
	case protoreflect.BytesKind:
		t = "BINARY"
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		t = "BIGINT"
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		t = "INT"
	case protoreflect.BoolKind:
		t = "BOOLEAN"
	case protoreflect.DoubleKind:
		t = "DOUBLE"
	case protoreflect.FloatKind:
		t = "FLOAT"
	default:
		panic(fmt.Sprintf("tablemgr: unsupported field kind %s for %s", f.Kind(), f.Name()))
	}
	if f.IsList() {
		return "ARRAY<" + t + ">"
	}
	return t
}

// Manager ensures tables exist with the expected columns.
type Manager struct {
	Tables TablesAPI
	SQL    *dbsql.Client
	Mode   Mode
	Logger *slog.Logger

	mu   sync.Mutex
	done map[string]bool
}

// Ensure makes table match the expected schema according to Mode. It is
// safe to call concurrently and from several replicas.
func (m *Manager) Ensure(ctx context.Context, table string) error {
	if m.Mode == ModeOff || m.Mode == "" {
		return nil
	}
	m.mu.Lock()
	if m.done[table] {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()

	missing, exists, err := m.diff(ctx, table)
	if err != nil {
		return err
	}
	switch {
	case !exists && m.Mode == ModeVerify:
		return fmt.Errorf("table %s does not exist (SFZB_SCHEMA_MODE=verify)", table)
	case !exists:
		if err := m.create(ctx, table); err != nil {
			return err
		}
	case len(missing) > 0 && m.Mode == ModeVerify:
		return fmt.Errorf("table %s is missing columns %s (SFZB_SCHEMA_MODE=verify)", table, names(missing))
	case len(missing) > 0:
		if err := m.addColumns(ctx, table, missing); err != nil {
			return err
		}
	}
	m.mu.Lock()
	if m.done == nil {
		m.done = map[string]bool{}
	}
	m.done[table] = true
	m.mu.Unlock()
	return nil
}

// diff returns expected columns missing from table. A column present with a
// different type is an error: it cannot be fixed additively.
func (m *Manager) diff(ctx context.Context, table string) (missing []Column, exists bool, err error) {
	info, err := m.Tables.Get(ctx, catalog.GetTableRequest{FullName: table})
	if errors.Is(err, apierr.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("describing %s: %w", table, err)
	}
	have := map[string]string{}
	for _, c := range info.Columns {
		have[strings.ToLower(c.Name)] = strings.ToUpper(strings.ReplaceAll(c.TypeText, " ", ""))
	}
	var mismatched []string
	for _, c := range Columns() {
		t, ok := have[c.Name]
		switch {
		case !ok:
			missing = append(missing, c)
		case !typesEqual(t, c.Type):
			mismatched = append(mismatched, fmt.Sprintf("%s is %s, want %s", c.Name, t, c.Type))
		}
	}
	if len(mismatched) > 0 {
		return nil, true, fmt.Errorf("table %s has incompatible columns: %s", table, strings.Join(mismatched, "; "))
	}
	return missing, true, nil
}

func typesEqual(have, want string) bool {
	aliases := map[string]string{"LONG": "BIGINT", "INTEGER": "INT"}
	norm := func(s string) string {
		for k, v := range aliases {
			s = strings.ReplaceAll(s, k, v)
		}
		return s
	}
	return norm(have) == norm(want)
}

// CreateTableDDL returns the CREATE TABLE statement for table.
func CreateTableDDL(table string) (string, error) {
	qt, err := dbsql.QuoteTable(table)
	if err != nil {
		return "", err
	}
	var cols []string
	for _, c := range Columns() {
		cols = append(cols, "  "+c.Name+" "+c.Type)
	}
	return "CREATE TABLE IF NOT EXISTS " + qt + " (\n" + strings.Join(cols, ",\n") + "\n)\nCLUSTER BY (org_id, entity_name)\n" +
		"TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true')", nil
}

func (m *Manager) create(ctx context.Context, table string) error {
	stmt, err := CreateTableDDL(table)
	if err != nil {
		return err
	}
	if _, err := m.SQL.Query(ctx, stmt); err != nil {
		return fmt.Errorf("creating %s: %w", table, err)
	}
	m.Logger.Info("Created Delta table", "table", table)
	// Another replica may have created it with an older schema first.
	missing, _, err := m.diff(ctx, table)
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		return m.addColumns(ctx, table, missing)
	}
	return nil
}

func (m *Manager) addColumns(ctx context.Context, table string, cols []Column) error {
	qt, err := dbsql.QuoteTable(table)
	if err != nil {
		return err
	}
	var defs []string
	for _, c := range cols {
		defs = append(defs, c.Name+" "+c.Type)
	}
	_, err = m.SQL.Query(ctx, "ALTER TABLE "+qt+" ADD COLUMNS ("+strings.Join(defs, ", ")+")")
	if err != nil {
		// A concurrent migration may have added them; re-check before failing.
		if missing, _, derr := m.diff(ctx, table); derr == nil && len(missing) == 0 {
			return nil
		}
		return fmt.Errorf("adding columns %s to %s: %w", names(cols), table, err)
	}
	m.Logger.Info("Added columns to Delta table", "table", table, "columns", names(cols))
	return nil
}

func names(cols []Column) string {
	n := make([]string, len(cols))
	for i, c := range cols {
		n[i] = c.Name
	}
	return strings.Join(n, ", ")
}
