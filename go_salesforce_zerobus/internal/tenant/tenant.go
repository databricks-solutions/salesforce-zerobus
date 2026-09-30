// Package tenant defines the tenant registry: Salesforce orgs, their
// subscriptions, and the sources that load them.
package tenant

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"time"
)

// Key identifies a tenant. It is stable, unique, and the shard hash input.
type Key string

// SubKey identifies one subscription: a tenant and a Pub/Sub topic.
type SubKey struct {
	Tenant Key
	Topic  string
}

func (k SubKey) String() string { return string(k.Tenant) + "|" + k.Topic }

// Preset is a Salesforce replay preset used when no checkpoint exists.
type Preset string

const (
	Latest   Preset = "LATEST"
	Earliest Preset = "EARLIEST"
)

// AuthType selects the Salesforce authentication flow.
type AuthType string

const (
	AuthOAuthClientCredentials AuthType = "oauth_client_credentials"
	AuthSOAP                   AuthType = "soap"
)

// Auth holds Salesforce credentials. Secrets are always references
// (env://, file://, uc-secret://catalog.schema.secret), never literals.
type Auth struct {
	Type             AuthType `yaml:"type"`
	ClientID         string   `yaml:"client_id"`
	ClientIDRef      string   `yaml:"client_id_ref"`
	ClientSecretRef  string   `yaml:"client_secret_ref"`
	Username         string   `yaml:"username"`
	UsernameRef      string   `yaml:"username_ref"`
	PasswordRef      string   `yaml:"password_ref"`
	SecurityTokenRef string   `yaml:"security_token_ref"`

	// Literal secrets are rejected by validation; they exist only so the
	// error message can point at the offending key.
	ClientSecret string `yaml:"client_secret"`
	Password     string `yaml:"password"`
}

// Salesforce is per-tenant Salesforce connectivity.
type Salesforce struct {
	InstanceURL string        `yaml:"instance_url"`
	APIVersion  string        `yaml:"api_version"`
	TokenTTL    time.Duration `yaml:"token_ttl"`
	Auth        Auth          `yaml:"auth"`
}

// SubscriptionSpec is one (tenant, topic) subscription after defaults merge.
type SubscriptionSpec struct {
	Key             SubKey
	Table           string
	ReplayDefault   Preset
	OnReplayExpired Preset
	BatchSize       int
	MaxUnacked      int
	DedupSize       int
}

// Tenant is one Salesforce org.
type Tenant struct {
	Key           Key
	ExpectedOrgID string
	Enabled       bool
	ShardPin      *int
	Labels        map[string]string
	Salesforce    Salesforce
	Subscriptions []SubscriptionSpec

	// Origin is "file:line" for error messages.
	Origin string
}

// Hash identifies the effective configuration of sub within t. A change means
// the running subscription must be restarted.
func (t Tenant) Hash(sub SubscriptionSpec) string {
	h := sha256.New()
	fmt.Fprintf(h, "%s|%s|%v|%+v|%+v", t.Key, t.ExpectedOrgID, t.Enabled, t.Salesforce, sub)
	return hex.EncodeToString(h.Sum(nil)[:12])
}

// Snapshot is a complete, validated tenant registry.
type Snapshot struct {
	Version string
	Tenants []Tenant
}

// Subscriptions returns every subscription of enabled tenants, sorted by key.
func (s Snapshot) Subscriptions() []SubscriptionSpec {
	var out []SubscriptionSpec
	for _, t := range s.Tenants {
		if t.Enabled {
			out = append(out, t.Subscriptions...)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key.String() < out[j].Key.String() })
	return out
}

// Tables returns the number of enabled subscriptions per target table.
func (s Snapshot) Tables() map[string]int {
	tables := map[string]int{}
	for _, sub := range s.Subscriptions() {
		tables[sub.Table]++
	}
	return tables
}

// Tenant returns the tenant with key k.
func (s Snapshot) Tenant(k Key) (Tenant, bool) {
	for _, t := range s.Tenants {
		if t.Key == k {
			return t, true
		}
	}
	return Tenant{}, false
}
