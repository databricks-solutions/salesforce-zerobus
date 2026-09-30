package config

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/tenant"
)

// LegacyTenant is a single tenant synthesized from the environment variables
// used by the Python service and go_salesforce_zerobus_cgo.
type LegacyTenant struct {
	Tenant tenant.Tenant
	// SeedFromDelta resumes from the latest replay ID in the Delta table when
	// no checkpoint exists yet (rows written by the old services).
	SeedFromDelta bool
}

// LoadLegacyTenant builds a tenant from SALESFORCE_* variables. ok is false
// when SALESFORCE_CHANGE_EVENT_CHANNEL is unset.
func LoadLegacyTenant(getenv Getenv) (LegacyTenant, bool, error) {
	channel := strings.TrimSpace(getenv("SALESFORCE_CHANGE_EVENT_CHANNEL"))
	if channel == "" {
		return LegacyTenant{}, false, nil
	}
	env := envReader{getenv: getenv}
	key := env.str("SFZB_LEGACY_TENANT_KEY", "default")
	table := env.str("DATABRICKS_ZEROBUS_TARGET_TABLE", "")

	var auth tenant.Auth
	if env.str("SALESFORCE_CLIENT_ID", "") != "" && env.str("SALESFORCE_CLIENT_SECRET", "") != "" {
		auth = tenant.Auth{
			Type:            tenant.AuthOAuthClientCredentials,
			ClientIDRef:     "env://SALESFORCE_CLIENT_ID",
			ClientSecretRef: "env://SALESFORCE_CLIENT_SECRET",
		}
	} else {
		auth = tenant.Auth{
			Type:        tenant.AuthSOAP,
			UsernameRef: "env://SALESFORCE_USERNAME",
			PasswordRef: "env://SALESFORCE_PASSWORD",
		}
		if env.str("SALESFORCE_TOKEN", "") != "" {
			auth.SecurityTokenRef = "env://SALESFORCE_TOKEN"
		}
	}

	replayDefault := tenant.Latest
	if b, err := strconv.ParseBool(env.str("DATABRICKS_BACKFILL_HISTORICAL", "true")); err == nil && b {
		replayDefault = tenant.Earliest
	}
	batch := min(max(env.int("SALESFORCE_BATCH_SIZE", 100), 1), 100)

	doc := fmt.Sprintf(`
version: 1
tenants:
  - key: %q
    salesforce:
      instance_url: %q
      api_version: %q
      auth:
        type: %s
        client_id_ref: %q
        client_secret_ref: %q
        username_ref: %q
        password_ref: %q
        security_token_ref: %q
    subscriptions:
      - topic: %q
        table: %q
        replay_default: %s
        batch_size: %d
`, key, env.str("SALESFORCE_INSTANCE_URL", ""), env.str("SALESFORCE_API_VERSION", "62.0"),
		auth.Type, auth.ClientIDRef, auth.ClientSecretRef, auth.UsernameRef, auth.PasswordRef, auth.SecurityTokenRef,
		"/data/"+strings.TrimPrefix(channel, "/data/"), table, replayDefault, batch)

	tenants, err := tenant.Parse("legacy environment", []byte(doc), tenant.LoadOptions{SecretSchemes: []string{"env"}}, tenant.BuiltinDefaults())
	if err != nil {
		return LegacyTenant{}, false, fmt.Errorf("single-tenant mode (SALESFORCE_* variables): %w", err)
	}
	if len(env.errs) > 0 {
		return LegacyTenant{}, false, env.errs[0]
	}
	seed, _ := strconv.ParseBool(env.str("SFZB_SEED_FROM_DELTA", "true"))
	return LegacyTenant{Tenant: tenants[0], SeedFromDelta: seed}, true, nil
}
