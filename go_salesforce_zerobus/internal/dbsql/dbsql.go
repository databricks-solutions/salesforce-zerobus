// Package dbsql runs statements on a Databricks SQL warehouse.
package dbsql

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/databricks/databricks-sdk-go/service/sql"
)

// StatementAPI is the subset of the Statement Execution API used here.
type StatementAPI interface {
	ExecuteAndWait(ctx context.Context, req sql.ExecuteStatementRequest) (*sql.StatementResponse, error)
}

// Client executes statements on one warehouse.
type Client struct {
	API         StatementAPI
	WarehouseID string
}

// Param is a named statement parameter (:name in the statement).
type Param struct{ Name, Value, Type string }

// Query runs statement and returns the inline result rows.
func (c *Client) Query(ctx context.Context, statement string, params ...Param) ([][]string, error) {
	if c.API == nil || c.WarehouseID == "" {
		return nil, fmt.Errorf("SQL warehouse is not configured (SFZB_WAREHOUSE_ID)")
	}
	req := sql.ExecuteStatementRequest{
		WarehouseId: c.WarehouseID,
		Statement:   statement,
		WaitTimeout: "30s",
		Disposition: sql.DispositionInline,
		Format:      sql.FormatJsonArray,
	}
	for _, p := range params {
		req.Parameters = append(req.Parameters, sql.StatementParameterListItem{Name: p.Name, Value: p.Value, Type: p.Type})
	}
	resp, err := c.API.ExecuteAndWait(ctx, req)
	if err != nil {
		return nil, err
	}
	if resp.Result == nil {
		return nil, nil
	}
	return resp.Result.DataArray, nil
}

var identPart = regexp.MustCompile("^[^`.\\s]+$")

// QuoteTable quotes a three-part table name for SQL.
func QuoteTable(name string) (string, error) {
	parts := strings.Split(name, ".")
	if len(parts) != 3 {
		return "", fmt.Errorf("table %q must be catalog.schema.table", name)
	}
	for i, p := range parts {
		if !identPart.MatchString(p) {
			return "", fmt.Errorf("table %q has an invalid identifier %q", name, p)
		}
		parts[i] = "`" + p + "`"
	}
	return strings.Join(parts, "."), nil
}
