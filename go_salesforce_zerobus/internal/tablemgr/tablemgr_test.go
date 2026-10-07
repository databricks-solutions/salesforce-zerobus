package tablemgr

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/databricks/databricks-sdk-go/apierr"
	"github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/databricks/databricks-sdk-go/service/sql"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/dbsql"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/event"
)

type fakeUC struct {
	cols  map[string][]catalog.ColumnInfo
	stmts []string
}

func (f *fakeUC) Get(_ context.Context, req catalog.GetTableRequest) (*catalog.TableInfo, error) {
	cols, ok := f.cols[req.FullName]
	if !ok {
		return nil, apierr.ErrResourceDoesNotExist
	}
	return &catalog.TableInfo{Columns: cols}, nil
}

func (f *fakeUC) ExecuteAndWait(_ context.Context, req sql.ExecuteStatementRequest) (*sql.StatementResponse, error) {
	f.stmts = append(f.stmts, req.Statement)
	return &sql.StatementResponse{}, nil
}

// tableColumns returns the first n columns as a table described by UC.
func tableColumns(n int) []catalog.ColumnInfo {
	var out []catalog.ColumnInfo
	for _, c := range Columns(event.FormatVariant)[:n] {
		out = append(out, catalog.ColumnInfo{Name: c.Name, TypeText: strings.ToLower(c.Type)})
	}
	return out
}

func legacyColumns() []catalog.ColumnInfo { return tableColumns(16) } // the original 16-column schema

func manager(f *fakeUC, mode Mode) *Manager {
	return &Manager{Tables: f, SQL: &dbsql.Client{API: f, WarehouseID: "wh"}, Mode: mode,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

func TestColumns(t *testing.T) {
	cols := Columns(event.FormatVariant)
	if len(cols) != 24 || cols[0] != (Column{"event_id", "STRING"}) || cols[23] != (Column{"record", "VARIANT"}) {
		t.Fatalf("columns = %v", cols)
	}
	want := map[string]string{"record_ids": "ARRAY<STRING>", "payload_binary": "BINARY", "timestamp": "BIGINT", "sequence_number": "INT", "topic": "STRING"}
	for _, c := range cols {
		if w, ok := want[c.Name]; ok && w != c.Type {
			t.Errorf("%s = %s, want %s", c.Name, c.Type, w)
		}
	}
	if avro := Columns(event.FormatAvro); len(avro) != 23 || avro[22].Name != "commit_user" {
		t.Fatalf("avro columns must omit record: %v", avro)
	}
}

func TestMigrateAddsVariantColumnOnlyForVariantLayouts(t *testing.T) {
	f := &fakeUC{cols: map[string][]catalog.ColumnInfo{"main.sf.events": tableColumns(23)}}
	m := manager(f, ModeMigrate)
	m.Format = event.FormatAvro
	if err := m.Ensure(context.Background(), "main.sf.events"); err != nil || len(f.stmts) != 0 {
		t.Fatalf("avro layout must not alter a 23-column table: err=%v stmts=%v", err, f.stmts)
	}
	if err := manager(f, ModeMigrate).Ensure(context.Background(), "main.sf.events"); err != nil {
		t.Fatal(err)
	}
	if len(f.stmts) != 1 || f.stmts[0] != "ALTER TABLE `main`.`sf`.`events` ADD COLUMNS (record VARIANT)" {
		t.Fatalf("stmts = %v", f.stmts)
	}
	f.cols["main.sf.full"] = tableColumns(24)
	if err := manager(f, ModeVerify).Ensure(context.Background(), "main.sf.full"); err != nil {
		t.Fatalf("variant column should match: %v", err)
	}
}

func TestMigrateAddsMissingColumns(t *testing.T) {
	f := &fakeUC{cols: map[string][]catalog.ColumnInfo{"main.sf.events": legacyColumns()}}
	m := manager(f, ModeMigrate)
	if err := m.Ensure(context.Background(), "main.sf.events"); err != nil {
		t.Fatal(err)
	}
	if len(f.stmts) != 1 || !strings.HasPrefix(f.stmts[0], "ALTER TABLE `main`.`sf`.`events` ADD COLUMNS (topic STRING, tenant_key STRING,") {
		t.Fatalf("stmts = %v", f.stmts)
	}
	m.Ensure(context.Background(), "main.sf.events")
	if len(f.stmts) != 1 {
		t.Error("second Ensure should be cached")
	}
}

func TestMigrateCreatesTable(t *testing.T) {
	f := &fakeUC{cols: map[string][]catalog.ColumnInfo{}}
	if err := manager(f, ModeMigrate).Ensure(context.Background(), "main.sf.new"); err != nil {
		// diff after create sees the table still missing in the fake; that is fine
		t.Fatal(err)
	}
	if len(f.stmts) == 0 || !strings.Contains(f.stmts[0], "CREATE TABLE IF NOT EXISTS `main`.`sf`.`new`") || !strings.Contains(f.stmts[0], "CLUSTER BY (org_id, entity_name)") ||
		!strings.Contains(f.stmts[0], "  record VARIANT\n)") || !strings.Contains(f.stmts[0], "'delta.enableVariantShredding' = 'true'") {
		t.Fatalf("stmts = %v", f.stmts)
	}
	avro, _ := CreateTableDDL("main.sf.old", event.FormatAvro)
	if strings.Contains(avro, "record VARIANT") || strings.Contains(avro, "Shredding") {
		t.Fatalf("avro DDL = %s", avro)
	}
}

func TestVerifyAndMismatch(t *testing.T) {
	f := &fakeUC{cols: map[string][]catalog.ColumnInfo{"main.sf.events": legacyColumns()}}
	if err := manager(f, ModeVerify).Ensure(context.Background(), "main.sf.events"); err == nil || !strings.Contains(err.Error(), "missing columns topic") {
		t.Fatalf("verify err = %v", err)
	}
	cols := legacyColumns()
	cols[3].TypeText = "string" // timestamp should be BIGINT
	f.cols["main.sf.bad"] = cols
	if err := manager(f, ModeMigrate).Ensure(context.Background(), "main.sf.bad"); err == nil || !strings.Contains(err.Error(), "timestamp is STRING, want BIGINT") {
		t.Fatalf("mismatch err = %v", err)
	}
	if err := manager(f, ModeOff).Ensure(context.Background(), "main.sf.bad"); err != nil {
		t.Fatal(err)
	}
}
