package tenant

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// LoadOptions controls validation.
type LoadOptions struct {
	// SecretSchemes are the registered secret reference schemes (e.g. "env").
	SecretSchemes []string
	// ShardCount bounds per-tenant shard pins. Zero skips the check.
	ShardCount int
}

// TenantInput is an unvalidated tenant, as written in YAML or stored in the
// registry tables. Zero values take defaults.
type TenantInput struct {
	Key           string              `yaml:"key"`
	OrgID         string              `yaml:"org_id"`
	Enabled       *bool               `yaml:"enabled"`
	Shard         *int                `yaml:"shard"`
	Labels        map[string]string   `yaml:"labels"`
	Salesforce    Salesforce          `yaml:"salesforce"`
	Subscriptions []SubscriptionInput `yaml:"subscriptions"`
}

// SubscriptionInput is an unvalidated subscription.
type SubscriptionInput struct {
	Topic           string `yaml:"topic"`
	Table           string `yaml:"table"`
	Enabled         *bool  `yaml:"enabled"`
	ReplayDefault   Preset `yaml:"replay_default"`
	OnReplayExpired Preset `yaml:"on_replay_expired"`
	BatchSize       int    `yaml:"batch_size"`
	MaxUnacked      int    `yaml:"max_unacked"`
	DedupSize       int    `yaml:"dedup_size"`
}

// Defaults fill unset tenant and subscription fields.
type Defaults struct {
	Table        string
	APIVersion   string
	TokenTTL     time.Duration
	Subscription SubscriptionInput
}

// BuiltinDefaults are the defaults when nothing else is configured.
func BuiltinDefaults() Defaults {
	return Defaults{
		APIVersion: "62.0",
		TokenTTL:   time.Hour,
		Subscription: SubscriptionInput{
			ReplayDefault:   Latest,
			OnReplayExpired: Earliest,
			BatchSize:       100,
			MaxUnacked:      500,
			DedupSize:       512,
		},
	}
}

// Over returns d with the non-zero fields of o applied on top.
func (d Defaults) Over(o Defaults) Defaults {
	out := d
	if o.Table != "" {
		out.Table = o.Table
	}
	if o.APIVersion != "" {
		out.APIVersion = o.APIVersion
	}
	if o.TokenTTL != 0 {
		out.TokenTTL = o.TokenTTL
	}
	out.Subscription = mergeSub(out.Subscription, o.Subscription)
	return out
}

const maxBatchSize = 100 // Pub/Sub API limit for num_requested

var (
	keyPattern   = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)
	topicPattern = regexp.MustCompile(`^/data/\w+$`)
	tablePattern = regexp.MustCompile("^[^.\\s`]+\\.[^.\\s`]+\\.[^.\\s`]+$")
	orgIDPattern = regexp.MustCompile(`^00D[A-Za-z0-9]{12}([A-Za-z0-9]{3})?$`)
)

// Build validates in and applies defaults.
func Build(in TenantInput, d Defaults, origin string, opts LoadOptions) (Tenant, error) {
	var errs []error
	fail := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf("%s: tenant %q: %s", origin, in.Key, fmt.Sprintf(format, args...)))
	}

	if !keyPattern.MatchString(in.Key) {
		fail("key must match %s", keyPattern)
	}
	if in.OrgID != "" && !orgIDPattern.MatchString(in.OrgID) {
		fail("org_id %q is not a Salesforce org ID (00D...)", in.OrgID)
	}
	if in.Shard != nil && (*in.Shard < 0 || (opts.ShardCount > 0 && *in.Shard >= opts.ShardCount)) {
		fail("shard %d out of range [0, %d)", *in.Shard, opts.ShardCount)
	}

	sf := in.Salesforce
	sf.InstanceURL = strings.TrimRight(sf.InstanceURL, "/")
	if !strings.HasPrefix(sf.InstanceURL, "https://") && !strings.HasPrefix(sf.InstanceURL, "http://") {
		fail("salesforce.instance_url must be an http(s) URL")
	}
	sf.APIVersion = firstNonEmpty(sf.APIVersion, d.APIVersion, "62.0")
	if sf.TokenTTL == 0 {
		sf.TokenTTL = d.TokenTTL
	}
	if sf.TokenTTL == 0 {
		sf.TokenTTL = time.Hour
	}
	if sf.TokenTTL < 5*time.Minute {
		fail("salesforce.token_ttl must be at least 5m")
	}
	for _, e := range validateAuth(sf.Auth, opts) {
		fail("salesforce.auth: %s", e)
	}

	t := Tenant{
		Key:           Key(in.Key),
		ExpectedOrgID: in.OrgID,
		Enabled:       in.Enabled == nil || *in.Enabled,
		ShardPin:      in.Shard,
		Labels:        in.Labels,
		Salesforce:    sf,
		Origin:        origin,
	}
	seen := map[string]bool{}
	for i, sd := range in.Subscriptions {
		m := mergeSub(d.Subscription, sd)
		m.Topic = sd.Topic
		m.Table = firstNonEmpty(sd.Table, d.Subscription.Table, d.Table)
		switch {
		case !topicPattern.MatchString(m.Topic):
			fail("subscriptions[%d].topic %q must look like /data/AccountChangeEvent", i, m.Topic)
		case seen[m.Topic]:
			fail("subscriptions[%d].topic %q is duplicated", i, m.Topic)
		}
		seen[m.Topic] = true
		if m.Table == "" {
			fail("subscriptions[%d]: no target table (set it on the subscription or a default table)", i)
		} else if !tablePattern.MatchString(m.Table) {
			fail("subscriptions[%d].table %q must be catalog.schema.table", i, m.Table)
		}
		if !validPreset(m.ReplayDefault) || !validPreset(m.OnReplayExpired) {
			fail("subscriptions[%d]: replay presets must be LATEST or EARLIEST", i)
		}
		if m.BatchSize < 1 || m.BatchSize > maxBatchSize {
			fail("subscriptions[%d].batch_size must be in [1, %d]", i, maxBatchSize)
		}
		if m.MaxUnacked < m.BatchSize {
			fail("subscriptions[%d].max_unacked must be >= batch_size", i)
		}
		if m.DedupSize < 1 {
			fail("subscriptions[%d].dedup_size must be >= 1", i)
		}
		if sd.Enabled != nil && !*sd.Enabled {
			continue
		}
		t.Subscriptions = append(t.Subscriptions, SubscriptionSpec{
			Key:             SubKey{Tenant: t.Key, Topic: m.Topic},
			Table:           m.Table,
			ReplayDefault:   m.ReplayDefault,
			OnReplayExpired: m.OnReplayExpired,
			BatchSize:       m.BatchSize,
			MaxUnacked:      m.MaxUnacked,
			DedupSize:       m.DedupSize,
		})
	}
	return t, errors.Join(errs...)
}

// ValidateSet checks cross-tenant uniqueness (keys, org IDs).
func ValidateSet(tenants []Tenant) error {
	var errs []error
	keys := map[Key]string{}
	orgs := map[string]string{}
	for _, t := range tenants {
		if prev, ok := keys[t.Key]; ok {
			errs = append(errs, fmt.Errorf("%s: tenant key %q already defined at %s", t.Origin, t.Key, prev))
		}
		keys[t.Key] = t.Origin
		if t.ExpectedOrgID != "" {
			if prev, ok := orgs[t.ExpectedOrgID]; ok {
				errs = append(errs, fmt.Errorf("%s: org_id %s already used at %s", t.Origin, t.ExpectedOrgID, prev))
			}
			orgs[t.ExpectedOrgID] = t.Origin
		}
	}
	return errors.Join(errs...)
}

// --- YAML format (import via `zerobus tenants apply`, and dev/test runs) ---

type fileDoc struct {
	Version  int         `yaml:"version"`
	Defaults defaultsDoc `yaml:"defaults"`
	Tenants  []yaml.Node `yaml:"tenants"`
}

type defaultsDoc struct {
	Table        string            `yaml:"table"`
	Salesforce   sfDefaultsDoc     `yaml:"salesforce"`
	Subscription SubscriptionInput `yaml:"subscription"`
}

type sfDefaultsDoc struct {
	APIVersion string        `yaml:"api_version"`
	TokenTTL   time.Duration `yaml:"token_ttl"`
}

// Document is a parsed tenants YAML file.
type Document struct {
	Defaults Defaults // the file's defaults block (unset fields are zero)
	Tenants  []TenantInput
	Origins  []string // file:line of each tenant
}

// ParseDocument parses a tenants YAML file without validating tenants.
func ParseDocument(name string, data []byte) (Document, error) {
	var doc fileDoc
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&doc); err != nil {
		return Document{}, fmt.Errorf("%s: %w", name, err)
	}
	if doc.Version != 1 {
		return Document{}, fmt.Errorf("%s: unsupported version %d (want 1)", name, doc.Version)
	}
	if doc.Defaults.Subscription.Topic != "" {
		return Document{}, fmt.Errorf("%s: defaults.subscription.topic is not allowed", name)
	}
	out := Document{Defaults: Defaults{
		Table:        doc.Defaults.Table,
		APIVersion:   doc.Defaults.Salesforce.APIVersion,
		TokenTTL:     doc.Defaults.Salesforce.TokenTTL,
		Subscription: doc.Defaults.Subscription,
	}}
	var errs []error
	for i := range doc.Tenants {
		node := &doc.Tenants[i]
		origin := fmt.Sprintf("%s:%d", name, node.Line)
		var in TenantInput
		if err := decodeStrict(node, &in); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", origin, err))
			continue
		}
		out.Tenants = append(out.Tenants, in)
		out.Origins = append(out.Origins, origin)
	}
	return out, errors.Join(errs...)
}

// Parse parses and validates a tenants YAML file; the file's defaults block
// applies on top of base.
func Parse(name string, data []byte, opts LoadOptions, base Defaults) ([]Tenant, error) {
	doc, err := ParseDocument(name, data)
	if err != nil {
		return nil, err
	}
	d := base.Over(doc.Defaults)
	var tenants []Tenant
	var errs []error
	for i, in := range doc.Tenants {
		t, err := Build(in, d, doc.Origins[i], opts)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		tenants = append(tenants, t)
	}
	return tenants, errors.Join(errs...)
}

// LoadFiles loads and validates local tenants files matching patterns.
func LoadFiles(patterns []string, opts LoadOptions, base Defaults) (Snapshot, error) {
	var files []string
	for _, p := range patterns {
		if p = strings.TrimSpace(p); p == "" {
			continue
		}
		matches, err := filepath.Glob(p)
		if err != nil {
			return Snapshot{}, fmt.Errorf("bad tenants file pattern %q: %w", p, err)
		}
		if len(matches) == 0 {
			return Snapshot{}, fmt.Errorf("tenants file pattern %q matched no files", p)
		}
		files = append(files, matches...)
	}
	sort.Strings(files)
	if len(files) == 0 {
		return Snapshot{}, errors.New("no tenants files given")
	}
	var all []Tenant
	var errs []error
	h := sha256.New()
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		h.Write([]byte(f))
		h.Write(data)
		tenants, err := Parse(f, data, opts, base)
		if err != nil {
			errs = append(errs, err)
		}
		all = append(all, tenants...)
	}
	if err := ValidateSet(all); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return Snapshot{}, errors.Join(errs...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Key < all[j].Key })
	return Snapshot{Version: hex.EncodeToString(h.Sum(nil)[:8]), Tenants: all}, nil
}

// decodeStrict decodes node rejecting unknown keys (Node.Decode ignores
// KnownFields, so re-encode through a strict decoder).
func decodeStrict(node *yaml.Node, out any) error {
	raw, err := yaml.Marshal(node)
	if err != nil {
		return err
	}
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	return dec.Decode(out)
}

func validateAuth(a Auth, opts LoadOptions) []string {
	var errs []string
	if a.ClientSecret != "" {
		errs = append(errs, "client_secret must not be a literal; use client_secret_ref")
	}
	if a.Password != "" {
		errs = append(errs, "password must not be a literal; use password_ref")
	}
	checkRef := func(field, ref string, required bool) {
		if ref == "" {
			if required {
				errs = append(errs, field+" is required")
			}
			return
		}
		scheme, _, ok := strings.Cut(ref, "://")
		if !ok || !contains(opts.SecretSchemes, scheme) {
			errs = append(errs, fmt.Sprintf("%s %q: unknown secret scheme (have %s)", field, ref, strings.Join(opts.SecretSchemes, ", ")))
		}
	}
	switch a.Type {
	case AuthOAuthClientCredentials:
		if (a.ClientID == "") == (a.ClientIDRef == "") {
			errs = append(errs, "exactly one of client_id or client_id_ref is required")
		}
		checkRef("client_id_ref", a.ClientIDRef, false)
		checkRef("client_secret_ref", a.ClientSecretRef, true)
	case AuthSOAP:
		if (a.Username == "") == (a.UsernameRef == "") {
			errs = append(errs, "exactly one of username or username_ref is required")
		}
		checkRef("username_ref", a.UsernameRef, false)
		checkRef("password_ref", a.PasswordRef, true)
		checkRef("security_token_ref", a.SecurityTokenRef, false)
	default:
		errs = append(errs, fmt.Sprintf("type must be %q or %q", AuthOAuthClientCredentials, AuthSOAP))
	}
	return errs
}

func mergeSub(base, over SubscriptionInput) SubscriptionInput {
	out := base
	if over.Table != "" {
		out.Table = over.Table
	}
	if over.ReplayDefault != "" {
		out.ReplayDefault = Preset(strings.ToUpper(string(over.ReplayDefault)))
	}
	if over.OnReplayExpired != "" {
		out.OnReplayExpired = Preset(strings.ToUpper(string(over.OnReplayExpired)))
	}
	if over.BatchSize != 0 {
		out.BatchSize = over.BatchSize
	}
	if over.MaxUnacked != 0 {
		out.MaxUnacked = over.MaxUnacked
	}
	if over.DedupSize != 0 {
		out.DedupSize = over.DedupSize
	}
	return out
}

func validPreset(p Preset) bool { return p == Latest || p == Earliest }

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
