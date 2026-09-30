package databricks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/databricks-solutions/go-salesforce-zerobus/internal/config"
)

// SubscriptionParams determines where to start the Salesforce subscription.
type SubscriptionParams struct {
	ReplayType string // "LATEST", "EARLIEST", "CUSTOM"
	ReplayID   string // hex string for CUSTOM
}

// ReplayManager queries Databricks for replay state and manages the target table.
type ReplayManager struct {
	cfg         *config.Config
	logger      *slog.Logger
	httpClient  *http.Client
	warehouseID string

	oauthToken    string
	tokenExpiry   time.Time
}

// NewReplayManager creates a new replay manager.
func NewReplayManager(cfg *config.Config, logger *slog.Logger) (*ReplayManager, error) {
	warehouseID, err := extractWarehouseID(cfg.DBSQLEndpoint)
	if err != nil {
		return nil, fmt.Errorf("extracting warehouse ID: %w", err)
	}

	return &ReplayManager{
		cfg:         cfg,
		logger:      logger,
		httpClient:  &http.Client{Timeout: 60 * time.Second},
		warehouseID: warehouseID,
	}, nil
}

var warehouseIDPattern = regexp.MustCompile(`/sql/1\.0/warehouses/([a-f0-9]+)`)

func extractWarehouseID(sqlEndpoint string) (string, error) {
	matches := warehouseIDPattern.FindStringSubmatch(sqlEndpoint)
	if len(matches) < 2 {
		return "", fmt.Errorf("cannot extract warehouse ID from SQL endpoint: %s (expected format: /sql/1.0/warehouses/{id})", sqlEndpoint)
	}
	return matches[1], nil
}

// GetSubscriptionParams determines the subscription start point.
func (rm *ReplayManager) GetSubscriptionParams(ctx context.Context) (*SubscriptionParams, error) {
	exists, err := rm.TableExists(ctx)
	if err != nil {
		rm.logger.Warn("Error checking table existence, defaulting to LATEST", "error", err)
		return &SubscriptionParams{ReplayType: "LATEST"}, nil
	}

	if !exists {
		if rm.cfg.AutoCreateTable {
			if err := rm.CreateTableIfNotExists(ctx); err != nil {
				rm.logger.Warn("Failed to create table, defaulting to LATEST", "error", err)
				return &SubscriptionParams{ReplayType: "LATEST"}, nil
			}
			if rm.cfg.BackfillHistorical {
				return &SubscriptionParams{ReplayType: "EARLIEST"}, nil
			}
			return &SubscriptionParams{ReplayType: "LATEST"}, nil
		}
		return &SubscriptionParams{ReplayType: "LATEST"}, nil
	}

	replayID, err := rm.GetLastReplayID(ctx)
	if err != nil {
		rm.logger.Warn("Failed to get last replay ID, defaulting to EARLIEST", "error", err)
		if rm.cfg.BackfillHistorical {
			return &SubscriptionParams{ReplayType: "EARLIEST"}, nil
		}
		return &SubscriptionParams{ReplayType: "LATEST"}, nil
	}

	if replayID != "" {
		rm.logger.Info("Resuming from last replay ID", "replay_id", replayID)
		return &SubscriptionParams{ReplayType: "CUSTOM", ReplayID: replayID}, nil
	}

	if rm.cfg.BackfillHistorical {
		return &SubscriptionParams{ReplayType: "EARLIEST"}, nil
	}
	return &SubscriptionParams{ReplayType: "LATEST"}, nil
}

// GetLastReplayID queries the Delta table for the most recent replay_id.
func (rm *ReplayManager) GetLastReplayID(ctx context.Context) (string, error) {
	query := fmt.Sprintf(
		"SELECT replay_id FROM %s ORDER BY timestamp DESC LIMIT 1",
		rm.cfg.DBTableName,
	)

	result, err := rm.executeSQL(ctx, query, 30*time.Second)
	if err != nil {
		return "", err
	}

	// Parse the result
	resultData, ok := result["result"].(map[string]interface{})
	if !ok {
		return "", nil
	}
	dataArray, ok := resultData["data_array"].([]interface{})
	if !ok || len(dataArray) == 0 {
		return "", nil
	}
	row, ok := dataArray[0].([]interface{})
	if !ok || len(row) == 0 {
		return "", nil
	}
	replayID := fmt.Sprintf("%v", row[0])
	if replayID == "<nil>" || replayID == "" {
		return "", nil
	}
	return replayID, nil
}

// TableExists checks if the target table exists in Databricks.
func (rm *ReplayManager) TableExists(ctx context.Context) (bool, error) {
	query := fmt.Sprintf("DESCRIBE TABLE %s", rm.cfg.DBTableName)
	result, err := rm.executeSQL(ctx, query, 30*time.Second)
	if err != nil {
		return false, nil // Table doesn't exist or query failed
	}
	if result == nil {
		return false, nil
	}
	statusField, ok := result["status"].(map[string]interface{})
	if !ok {
		return false, nil
	}
	state, _ := statusField["state"].(string)
	return state == "SUCCEEDED", nil
}

// CreateTableIfNotExists creates the CDC events Delta table.
func (rm *ReplayManager) CreateTableIfNotExists(ctx context.Context) error {
	query := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    event_id STRING,
    schema_id STRING,
    replay_id STRING,
    timestamp BIGINT,
    change_type STRING,
    entity_name STRING,
    change_origin STRING,
    record_ids ARRAY<STRING>,
    changed_fields ARRAY<STRING>,
    nulled_fields ARRAY<STRING>,
    diff_fields ARRAY<STRING>,
    record_data_json STRING,
    payload_binary BINARY,
    schema_json STRING,
    org_id STRING,
    processed_timestamp BIGINT
) USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
)`, rm.cfg.DBTableName)

	_, err := rm.executeSQL(ctx, query, 30*time.Second)
	if err != nil {
		return fmt.Errorf("creating table: %w", err)
	}

	rm.logger.Info("Created Delta table", "table", rm.cfg.DBTableName)
	return nil
}

// executeSQL executes a SQL statement via the Databricks SQL Statement API.
func (rm *ReplayManager) executeSQL(ctx context.Context, query string, timeout time.Duration) (map[string]interface{}, error) {
	if err := rm.ensureToken(ctx); err != nil {
		return nil, fmt.Errorf("ensuring OAuth token: %w", err)
	}

	waitTimeout := timeout
	if waitTimeout > 50*time.Second {
		waitTimeout = 50 * time.Second
	}

	payload := map[string]interface{}{
		"warehouse_id": rm.warehouseID,
		"statement":    query,
		"wait_timeout": fmt.Sprintf("%ds", int(waitTimeout.Seconds())),
		"disposition":  "INLINE",
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshaling SQL request: %w", err)
	}

	url := strings.TrimRight(rm.cfg.DBWorkspaceURL, "/") + "/api/2.0/sql/statements"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("creating SQL request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+rm.oauthToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := rm.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("SQL request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading SQL response: %w", err)
	}

	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("parsing SQL response: %w", err)
	}

	// Check for errors
	if statusField, ok := result["status"].(map[string]interface{}); ok {
		state, _ := statusField["state"].(string)
		if state == "FAILED" {
			errMsg := ""
			if errInfo, ok := statusField["error"].(map[string]interface{}); ok {
				errMsg, _ = errInfo["message"].(string)
			}
			return nil, fmt.Errorf("SQL query failed: %s", errMsg)
		}
	}

	return result, nil
}

// ensureToken refreshes the OAuth token if expired or missing.
func (rm *ReplayManager) ensureToken(ctx context.Context) error {
	if rm.oauthToken != "" && time.Now().Before(rm.tokenExpiry) {
		return nil
	}
	return rm.generateOAuthToken(ctx)
}

// generateOAuthToken fetches a new OAuth token from the Databricks workspace.
func (rm *ReplayManager) generateOAuthToken(ctx context.Context) error {
	tokenURL := strings.TrimRight(rm.cfg.DBWorkspaceURL, "/") + "/oidc/v1/token"

	form := "grant_type=client_credentials&scope=all-apis"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(form))
	if err != nil {
		return fmt.Errorf("creating token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(rm.cfg.DBClientID, rm.cfg.DBClientSecret)

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("token request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("reading token response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("token request returned %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return fmt.Errorf("parsing token response: %w", err)
	}

	if tokenResp.AccessToken == "" {
		return fmt.Errorf("token response missing access_token")
	}

	rm.oauthToken = tokenResp.AccessToken
	// Buffer 5 minutes before actual expiry
	rm.tokenExpiry = time.Now().Add(time.Duration(tokenResp.ExpiresIn)*time.Second - 5*time.Minute)

	rm.logger.Debug("Databricks OAuth token refreshed", "expires_in", tokenResp.ExpiresIn)
	return nil
}
