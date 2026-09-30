package tenant

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var opts = LoadOptions{SecretSchemes: []string{"env", "file", "uc-secret"}, ShardCount: 4}

const validDoc = `
version: 1
defaults:
  table: main.salesforce.cdc_events
  salesforce: { api_version: "61.0" }
  subscription: { batch_size: 50 }
tenants:
  - key: acme-prod
    org_id: 00D5g000000AbCdEAA
    labels: { tier: gold }
    salesforce:
      instance_url: https://acme.my.salesforce.com/
      auth:
        type: oauth_client_credentials
        client_id: 3MVG9abc
        client_secret_ref: uc-secret://main.salesforce_zerobus_secrets.acme_prod#client_secret
    subscriptions:
      - topic: /data/ChangeEvents
      - topic: /data/OpportunityChangeEvent
        table: main.sales.opportunity_cdc
        replay_default: earliest
  - key: globex
    enabled: false
    shard: 3
    salesforce:
      instance_url: https://globex.my.salesforce.com
      token_ttl: 30m
      auth: { type: soap, username_ref: env://GLOBEX_USER, password_ref: file:///run/secrets/globex }
    subscriptions: [ { topic: /data/AccountChangeEvent } ]
`

func TestParseValid(t *testing.T) {
	tenants, err := Parse("t.yaml", []byte(validDoc), opts, BuiltinDefaults())
	if err != nil {
		t.Fatal(err)
	}
	if len(tenants) != 2 {
		t.Fatalf("got %d tenants", len(tenants))
	}
	acme := tenants[0]
	if acme.Salesforce.InstanceURL != "https://acme.my.salesforce.com" {
		t.Errorf("trailing slash not trimmed: %q", acme.Salesforce.InstanceURL)
	}
	if acme.Salesforce.APIVersion != "61.0" || acme.Salesforce.TokenTTL != time.Hour {
		t.Errorf("salesforce defaults not applied: %+v", acme.Salesforce)
	}
	s0, s1 := acme.Subscriptions[0], acme.Subscriptions[1]
	if s0.Table != "main.salesforce.cdc_events" || s0.BatchSize != 50 || s0.ReplayDefault != Latest || s0.OnReplayExpired != Earliest || s0.MaxUnacked != 500 {
		t.Errorf("defaults not merged: %+v", s0)
	}
	if s1.Table != "main.sales.opportunity_cdc" || s1.ReplayDefault != Earliest {
		t.Errorf("overrides not applied: %+v", s1)
	}
	globex := tenants[1]
	if globex.Enabled || globex.ShardPin == nil || *globex.ShardPin != 3 || globex.Salesforce.TokenTTL != 30*time.Minute {
		t.Errorf("globex = %+v", globex)
	}
	if !strings.HasPrefix(globex.Origin, "t.yaml:") {
		t.Errorf("origin = %q", globex.Origin)
	}
	snap := Snapshot{Tenants: tenants}
	if subs := snap.Subscriptions(); len(subs) != 2 {
		t.Errorf("disabled tenant subscriptions should be excluded: %d", len(subs))
	}
	if acme.Hash(s0) == acme.Hash(s1) {
		t.Error("distinct subscriptions should hash differently")
	}
}

func TestParseErrors(t *testing.T) {
	cases := map[string]struct{ doc, want string }{
		"literal secret": {`
version: 1
tenants:
  - key: a
    salesforce: { instance_url: https://a.example.com, auth: { type: oauth_client_credentials, client_id: x, client_secret: hunter2 } }
    subscriptions: [ { topic: /data/AccountChangeEvent, table: a.b.c } ]`, "client_secret must not be a literal"},
		"unknown key": {`
version: 1
tenants:
  - key: a
    bogus: true
    salesforce: { instance_url: https://a.example.com, auth: { type: soap, username: u, password_ref: env://P } }
    subscriptions: [ { topic: /data/AccountChangeEvent, table: a.b.c } ]`, "field bogus not found"},
		"bad scheme": {`
version: 1
tenants:
  - key: a
    salesforce: { instance_url: https://a.example.com, auth: { type: soap, username: u, password_ref: vault://x } }
    subscriptions: [ { topic: /data/AccountChangeEvent, table: a.b.c } ]`, "unknown secret scheme"},
		"bad topic and table": {`
version: 1
tenants:
  - key: a
    salesforce: { instance_url: https://a.example.com, auth: { type: soap, username: u, password_ref: env://P } }
    subscriptions: [ { topic: AccountChangeEvent, table: nodots } ]`, "must look like /data/"},
		"batch too large": {`
version: 1
tenants:
  - key: a
    salesforce: { instance_url: https://a.example.com, auth: { type: soap, username: u, password_ref: env://P } }
    subscriptions: [ { topic: /data/X, table: a.b.c, batch_size: 500 } ]`, "batch_size must be in [1, 100]"},
		"shard out of range": {`
version: 1
tenants:
  - key: a
    shard: 9
    salesforce: { instance_url: https://a.example.com, auth: { type: soap, username: u, password_ref: env://P } }
    subscriptions: [ { topic: /data/X, table: a.b.c } ]`, "shard 9 out of range"},
		"bad version": {"version: 2\ntenants: []", "unsupported version"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Parse("t.yaml", []byte(tc.doc), opts, BuiltinDefaults())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want containing %q", err, tc.want)
			}
		})
	}
}

func TestLoadFilesDuplicatesAcrossFiles(t *testing.T) {
	dir := t.TempDir()
	one := `
version: 1
defaults: { table: a.b.c }
tenants:
  - key: dup
    org_id: 00D5g000000AbCdEAA
    salesforce: { instance_url: https://a.example.com, auth: { type: soap, username: u, password_ref: env://P } }
    subscriptions: [ { topic: /data/X } ]`
	os.WriteFile(filepath.Join(dir, "1.yaml"), []byte(one), 0o600)
	os.WriteFile(filepath.Join(dir, "2.yaml"), []byte(one), 0o600)
	_, err := LoadFiles([]string{filepath.Join(dir, "*.yaml")}, opts, BuiltinDefaults())
	if err == nil || !strings.Contains(err.Error(), `tenant key "dup" already defined`) || !strings.Contains(err.Error(), "org_id 00D5g000000AbCdEAA already used") {
		t.Fatalf("err = %v", err)
	}

	os.Remove(filepath.Join(dir, "2.yaml"))
	snap, err := LoadFiles([]string{filepath.Join(dir, "*.yaml")}, opts, BuiltinDefaults())
	if err != nil || len(snap.Tenants) != 1 || snap.Version == "" {
		t.Fatalf("snap=%+v err=%v", snap, err)
	}
	if got := snap.Tables(); got["a.b.c"] != 1 {
		t.Errorf("Tables() = %v", got)
	}

	if _, err := LoadFiles([]string{filepath.Join(dir, "missing-*.yaml")}, opts, BuiltinDefaults()); err == nil {
		t.Fatal("expected error for pattern with no matches")
	}
}
