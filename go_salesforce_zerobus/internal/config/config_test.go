package config

import (
	"strings"
	"testing"
	"time"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/tenant"
)

func envMap(m map[string]string) Getenv { return func(k string) string { return m[k] } }

func TestLoadDefaultsAndFlags(t *testing.T) {
	s, err := Load([]string{"-shard-count", "4", "-shard-index", "2"}, envMap(map[string]string{
		"SFZB_DEFAULT_TABLE":          "main.salesforce.cdc_events",
		"DATABRICKS_HOST":             "https://ws.cloud.databricks.com",
		"DATABRICKS_CLIENT_ID":        "sp",
		"DATABRICKS_CLIENT_SECRET":    "secret",
		"SFZB_ZEROBUS_ENDPOINT":       "https://123.zerobus.us-west-2.cloud.databricks.com",
		"SFZB_LAKEBASE_ENDPOINT":      "projects/p/branches/production/endpoints/primary",
		"SFZB_WAREHOUSE_ID":           "abc123",
		"SFZB_CHECKPOINT_INTERVAL":    "10s",
		"ZEROBUS_RECOVERY_TIMEOUT_MS": "1500",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if s.TenantSource != "lakebase" || s.ShardCount != 4 || s.ShardIndex != "2" || s.Legacy {
		t.Errorf("unexpected %+v", s)
	}
	if s.CheckpointStore != "lakebase" || s.CheckpointInterval != 10*time.Second || s.UCEndpoint != s.DatabricksHost {
		t.Errorf("defaults: store=%s interval=%v uc=%s", s.CheckpointStore, s.CheckpointInterval, s.UCEndpoint)
	}
	if s.ZBRecoveryTimeout != 1500*time.Millisecond || s.SFPubSubAddr != "api.pubsub.salesforce.com:7443" {
		t.Errorf("legacy ms / pubsub addr: %v %s", s.ZBRecoveryTimeout, s.SFPubSubAddr)
	}
	if err := s.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if d := s.Defaults(); d.Table != "main.salesforce.cdc_events" || d.Subscription.BatchSize != 100 || d.Subscription.ReplayDefault != "LATEST" {
		t.Errorf("Defaults() = %+v", d)
	}
	if got := s.StreamsPerTable(1200); got != 5 {
		t.Errorf("StreamsPerTable(1200) = %d", got)
	}
	if got := s.StreamsPerTable(100000); got != 16 {
		t.Errorf("StreamsPerTable cap = %d", got)
	}
}

func TestFileSourceRequiresFile(t *testing.T) {
	if _, err := Load([]string{"-tenant-source", "file"}, envMap(nil)); err == nil {
		t.Fatal("expected error without SFZB_TENANTS_FILE")
	}
	s, err := Load([]string{"-tenant-source", "file", "-tenants-file", "a.yaml,b/*.yaml"}, envMap(nil))
	if err != nil || len(s.TenantsFiles) != 2 {
		t.Fatalf("s=%+v err=%v", s, err)
	}
}

func TestLoadBadEnv(t *testing.T) {
	_, err := Load(nil, envMap(map[string]string{"SFZB_SHARD_COUNT": "many"}))
	if err == nil || !strings.Contains(err.Error(), "SFZB_SHARD_COUNT") {
		t.Fatalf("err = %v", err)
	}
}

func TestValidateReportsMissing(t *testing.T) {
	s, err := Load(nil, envMap(map[string]string{}))
	if err != nil {
		t.Fatal(err)
	}
	err = s.Validate()
	for _, want := range []string{"DATABRICKS_HOST", "DATABRICKS_CLIENT_SECRET", "SFZB_ZEROBUS_ENDPOINT", "SFZB_LAKEBASE_ENDPOINT"} {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("Validate() should mention %s: %v", want, err)
		}
	}
}

func TestLegacyTenant(t *testing.T) {
	env := map[string]string{
		"SALESFORCE_CHANGE_EVENT_CHANNEL": "AccountChangeEvent",
		"SALESFORCE_INSTANCE_URL":         "https://acme.my.salesforce.com",
		"SALESFORCE_USERNAME":             "u",
		"SALESFORCE_PASSWORD":             "p",
		"SALESFORCE_TOKEN":                "tok",
		"SALESFORCE_BATCH_SIZE":           "10",
		"DATABRICKS_WORKSPACE_URL":        "https://ws.cloud.databricks.com",
		"DATABRICKS_INGEST_ENDPOINT":      "https://1.zerobus.us-west-2.cloud.databricks.com",
		"DATABRICKS_SQL_ENDPOINT":         "/sql/1.0/warehouses/abc123def",
		"DATABRICKS_ZEROBUS_TARGET_TABLE": "main.sf.account_events",
		"DATABRICKS_CLIENT_ID":            "sp",
		"DATABRICKS_CLIENT_SECRET":        "s",
	}
	s, err := Load(nil, envMap(env))
	if err != nil {
		t.Fatal(err)
	}
	if !s.Legacy || s.TenantSource != "env" || s.CheckpointStore != "delta" || s.WarehouseID != "abc123def" || s.DatabricksHost != env["DATABRICKS_WORKSPACE_URL"] {
		t.Fatalf("legacy settings: %+v", s)
	}
	lt := s.LegacyTenant.Tenant
	sub := lt.Subscriptions[0]
	if lt.Key != "default" || sub.Key.Topic != "/data/AccountChangeEvent" || sub.Table != "main.sf.account_events" {
		t.Errorf("tenant = %+v", lt)
	}
	if lt.Salesforce.Auth.Type != tenant.AuthSOAP || lt.Salesforce.Auth.SecurityTokenRef != "env://SALESFORCE_TOKEN" {
		t.Errorf("auth = %+v", lt.Salesforce.Auth)
	}
	if sub.ReplayDefault != tenant.Earliest || sub.BatchSize != 10 {
		t.Errorf("sub = %+v", sub)
	}
	if err := s.Validate(); err != nil {
		t.Errorf("Validate: %v", err)
	}

	env["SALESFORCE_CLIENT_ID"], env["SALESFORCE_CLIENT_SECRET"] = "cid", "csecret"
	s, _ = Load(nil, envMap(env))
	if s.LegacyTenant.Tenant.Salesforce.Auth.Type != tenant.AuthOAuthClientCredentials {
		t.Errorf("expected OAuth when client credentials are set")
	}
}
