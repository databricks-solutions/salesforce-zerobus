// Package pgtest provides a Postgres pool for tests that exercise real SQL.
// Tests are skipped unless SFZB_TEST_PG_DSN is set (see `make test-pg`).
package pgtest

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Pool returns a pool and a unique schema name, dropped at test end.
func Pool(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()
	dsn := os.Getenv("SFZB_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("SFZB_TEST_PG_DSN not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("sfzb_test_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		pool.Close()
	})
	return pool, schema
}
