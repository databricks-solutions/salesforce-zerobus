package delta

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/databricks/databricks-sdk-go/service/sql"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/checkpoint"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/dbsql"
)

type fakeSQL struct {
	stmts []string
	rows  [][]string
	errs  []error
}

func (f *fakeSQL) ExecuteAndWait(_ context.Context, req sql.ExecuteStatementRequest) (*sql.StatementResponse, error) {
	f.stmts = append(f.stmts, req.Statement)
	if len(f.errs) > 0 {
		err := f.errs[0]
		f.errs = f.errs[1:]
		if err != nil {
			return nil, err
		}
	}
	return &sql.StatementResponse{Result: &sql.ResultData{DataArray: f.rows}}, nil
}

func TestLoadMany(t *testing.T) {
	k := checkpoint.Key{Tenant: "default", Topic: "/data/AccountChangeEvent"}
	f := &fakeSQL{rows: [][]string{{"00D1", "002a"}}}
	s := &Store{SQL: &dbsql.Client{API: f, WarehouseID: "wh"}, IncludeLegacyRows: true,
		TableFor: func(checkpoint.Key) (string, bool) { return "main.sf.events", true }}
	got, err := s.LoadMany(context.Background(), []checkpoint.Key{k})
	if err != nil {
		t.Fatal(err)
	}
	if cp := got[k]; !bytes.Equal(cp.ReplayID, []byte{0x00, 0x2a}) || cp.OrgID != "00D1" {
		t.Fatalf("cp = %+v", cp)
	}
	if !strings.Contains(f.stmts[0], "tenant_key IS NULL AND topic IS NULL") || !strings.Contains(f.stmts[0], "`main`.`sf`.`events`") {
		t.Errorf("stmt = %s", f.stmts[0])
	}

	// Old table without the multi-tenant columns: falls back to the legacy query.
	f = &fakeSQL{rows: [][]string{{"00D1", "0001"}}, errs: []error{errors.New("[UNRESOLVED_COLUMN.WITH_SUGGESTION] tenant_key cannot be resolved"), nil}}
	s.SQL.API = f
	if got, err := s.LoadMany(context.Background(), []checkpoint.Key{k}); err != nil || len(got) != 1 {
		t.Fatalf("legacy fallback: %v %v", got, err)
	}
	if !strings.Contains(f.stmts[1], "ORDER BY timestamp DESC") {
		t.Errorf("fallback stmt = %s", f.stmts[1])
	}

	// Missing table means no checkpoint yet.
	s.SQL.API = &fakeSQL{errs: []error{errors.New("[TABLE_OR_VIEW_NOT_FOUND] nope")}}
	if got, err := s.LoadMany(context.Background(), []checkpoint.Key{k}); err != nil || len(got) != 0 {
		t.Fatalf("missing table: %v %v", got, err)
	}
}
