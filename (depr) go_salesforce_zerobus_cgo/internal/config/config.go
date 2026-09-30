package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// AuthMode determines which Salesforce authentication method to use.
type AuthMode int

const (
	AuthOAuth AuthMode = iota
	AuthSOAP
)

// Config holds all configuration for the salesforce-zerobus pipeline.
type Config struct {
	// Salesforce connection
	SFObjectChannel string
	SFInstanceURL   string
	SFClientID      string // OAuth
	SFClientSecret  string // OAuth
	SFUsername       string // SOAP
	SFPassword       string // SOAP
	GRPCHost        string
	GRPCPort        int
	APIVersion      string
	BatchSize       int
	TimeoutSeconds  float64
	MaxRetries      int // 0 = unlimited

	// Databricks connection
	DBWorkspaceURL   string
	DBClientID       string
	DBClientSecret   string
	DBIngestEndpoint string
	DBSQLEndpoint    string
	DBTableName      string

	// Table management
	AutoCreateTable    bool
	BackfillHistorical bool

	// Zerobus SDK configuration
	ZerobusMaxInflight        int
	ZerobusRecoveryRetries    int
	ZerobusRecoveryTimeoutMs  int
	ZerobusRecoveryBackoffMs  int
	ZerobusServerAckTimeoutMs int
	ZerobusFlushTimeoutMs     int

	// Pipeline tuning
	EventChannelSize int
}

// GetAuthMode returns the authentication mode based on available credentials.
func (c *Config) GetAuthMode() AuthMode {
	if c.SFClientID != "" && c.SFClientSecret != "" {
		return AuthOAuth
	}
	return AuthSOAP
}

// Topic returns the Salesforce Pub/Sub API topic path.
func (c *Config) Topic() string {
	return "/data/" + c.SFObjectChannel
}

// ObjectName extracts the Salesforce object name from the channel name.
func (c *Config) ObjectName() string {
	name := c.SFObjectChannel
	name = strings.TrimSuffix(name, "ChangeEvent")
	name = strings.TrimSuffix(name, "__")
	return name
}

// GRPCAddress returns the full gRPC endpoint address.
func (c *Config) GRPCAddress() string {
	return fmt.Sprintf("%s:%d", c.GRPCHost, c.GRPCPort)
}

// Load reads configuration from environment variables and applies defaults.
func Load() (*Config, error) {
	cfg := &Config{
		SFObjectChannel: os.Getenv("SALESFORCE_CHANGE_EVENT_CHANNEL"),
		SFInstanceURL:   os.Getenv("SALESFORCE_INSTANCE_URL"),
		SFClientID:      os.Getenv("SALESFORCE_CLIENT_ID"),
		SFClientSecret:  os.Getenv("SALESFORCE_CLIENT_SECRET"),
		SFUsername:       os.Getenv("SALESFORCE_USERNAME"),
		SFPassword:       os.Getenv("SALESFORCE_PASSWORD"),
		GRPCHost:        envOrDefault("SALESFORCE_GRPC_HOST", "api.pubsub.salesforce.com"),
		GRPCPort:        envIntOrDefault("SALESFORCE_GRPC_PORT", 7443),
		APIVersion:      envOrDefault("SALESFORCE_API_VERSION", "57.0"),
		BatchSize:       envIntOrDefault("SALESFORCE_BATCH_SIZE", 10),
		TimeoutSeconds:  envFloatOrDefault("SALESFORCE_TIMEOUT_SECONDS", 50.0),
		MaxRetries:      envIntOrDefault("SALESFORCE_MAX_RETRIES", 0),

		DBWorkspaceURL:   os.Getenv("DATABRICKS_WORKSPACE_URL"),
		DBClientID:       os.Getenv("DATABRICKS_CLIENT_ID"),
		DBClientSecret:   os.Getenv("DATABRICKS_CLIENT_SECRET"),
		DBIngestEndpoint: os.Getenv("DATABRICKS_INGEST_ENDPOINT"),
		DBSQLEndpoint:    os.Getenv("DATABRICKS_SQL_ENDPOINT"),
		DBTableName:      os.Getenv("DATABRICKS_ZEROBUS_TARGET_TABLE"),

		AutoCreateTable:    envBoolOrDefault("DATABRICKS_AUTO_CREATE_TABLE", true),
		BackfillHistorical: envBoolOrDefault("DATABRICKS_BACKFILL_HISTORICAL", true),

		ZerobusMaxInflight:        envIntOrDefault("ZEROBUS_MAX_INFLIGHT_RECORDS", 50000),
		ZerobusRecoveryRetries:    envIntOrDefault("ZEROBUS_RECOVERY_RETRIES", 5),
		ZerobusRecoveryTimeoutMs:  envIntOrDefault("ZEROBUS_RECOVERY_TIMEOUT_MS", 30000),
		ZerobusRecoveryBackoffMs:  envIntOrDefault("ZEROBUS_RECOVERY_BACKOFF_MS", 5000),
		ZerobusServerAckTimeoutMs: envIntOrDefault("ZEROBUS_SERVER_ACK_TIMEOUT_MS", 60000),
		ZerobusFlushTimeoutMs:     envIntOrDefault("ZEROBUS_FLUSH_TIMEOUT_MS", 300000),

		EventChannelSize: envIntOrDefault("EVENT_CHANNEL_SIZE", 1000),
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *Config) validate() error {
	if c.SFObjectChannel == "" {
		return fmt.Errorf("SALESFORCE_CHANGE_EVENT_CHANNEL is required")
	}
	if c.SFInstanceURL == "" {
		return fmt.Errorf("SALESFORCE_INSTANCE_URL is required")
	}

	hasOAuth := c.SFClientID != "" && c.SFClientSecret != ""
	hasSOAP := c.SFUsername != "" && c.SFPassword != ""
	if !hasOAuth && !hasSOAP {
		return fmt.Errorf("provide either (SALESFORCE_CLIENT_ID + SALESFORCE_CLIENT_SECRET) or (SALESFORCE_USERNAME + SALESFORCE_PASSWORD)")
	}

	if c.DBWorkspaceURL == "" {
		return fmt.Errorf("DATABRICKS_WORKSPACE_URL is required")
	}
	if c.DBClientID == "" || c.DBClientSecret == "" {
		return fmt.Errorf("DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET are required")
	}
	if c.DBIngestEndpoint == "" {
		return fmt.Errorf("DATABRICKS_INGEST_ENDPOINT is required")
	}
	if c.DBTableName == "" {
		return fmt.Errorf("DATABRICKS_ZEROBUS_TARGET_TABLE is required")
	}
	return nil
}

func envOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func envIntOrDefault(key string, defaultVal int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return defaultVal
}

func envFloatOrDefault(key string, defaultVal float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return defaultVal
}

func envBoolOrDefault(key string, defaultVal bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return defaultVal
}
