// Package secrets resolves secret references such as env://VAR,
// file:///path, and uc-secret://catalog.schema.secret (Unity Catalog).
package secrets

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/databricks/databricks-sdk-go/apierr"
	"github.com/databricks/databricks-sdk-go/service/catalog"
	"golang.org/x/sync/singleflight"
	"golang.org/x/time/rate"
)

// Provider resolves references for one scheme. path is everything after
// "scheme://".
type Provider interface {
	Scheme() string
	Resolve(ctx context.Context, path string) (string, error)
}

// Resolver resolves references through registered providers and caches
// values for a TTL, so thousands of tenants do not hammer a remote secret
// store. Concurrent lookups of the same reference share one call.
type Resolver struct {
	providers map[string]Provider
	ttl       time.Duration
	now       func() time.Time

	mu    sync.Mutex
	cache map[string]cached
	group singleflight.Group
}

type cached struct {
	value   string
	expires time.Time
}

// NewResolver creates a resolver. ttl <= 0 disables caching.
func NewResolver(ttl time.Duration, providers ...Provider) *Resolver {
	r := &Resolver{providers: map[string]Provider{}, ttl: ttl, now: time.Now, cache: map[string]cached{}}
	for _, p := range providers {
		r.providers[p.Scheme()] = p
	}
	return r
}

// Schemes returns the registered schemes, sorted.
func (r *Resolver) Schemes() []string {
	out := make([]string, 0, len(r.providers))
	for s := range r.providers {
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// Resolve returns the secret value for ref. A "#key" suffix selects a
// string field from a JSON-object secret (e.g.
// uc-secret://main.sfzb_secrets.acme_prod#client_secret), so one secret can hold all
// of a tenant's credentials. The whole secret is cached once per base ref.
func (r *Resolver) Resolve(ctx context.Context, ref string) (string, error) {
	base, field, hasField := strings.Cut(ref, "#")
	raw, err := r.resolveBase(ctx, base)
	if err != nil {
		return "", err
	}
	if !hasField {
		return raw, nil
	}
	var obj map[string]any
	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		return "", fmt.Errorf("secret %s is not a JSON object (needed for #%s)", redact(base), field)
	}
	v, ok := obj[field]
	if !ok {
		return "", fmt.Errorf("secret %s has no key %q", redact(base), field)
	}
	if str, ok := v.(string); ok {
		return str, nil
	}
	return fmt.Sprint(v), nil
}

func (r *Resolver) resolveBase(ctx context.Context, ref string) (string, error) {
	scheme, path, ok := strings.Cut(ref, "://")
	if !ok || path == "" {
		return "", fmt.Errorf("invalid secret reference %q (want scheme://path)", redact(ref))
	}
	p, ok := r.providers[scheme]
	if !ok {
		return "", fmt.Errorf("secret reference %q: unknown scheme %q", redact(ref), scheme)
	}
	if v, ok := r.lookup(ref); ok {
		return v, nil
	}
	v, err, _ := r.group.Do(ref, func() (any, error) {
		val, err := p.Resolve(ctx, path)
		if err != nil {
			return "", fmt.Errorf("resolving secret %s: %w", redact(ref), err)
		}
		if r.ttl > 0 {
			r.mu.Lock()
			r.cache[ref] = cached{value: val, expires: r.now().Add(r.ttl)}
			r.mu.Unlock()
		}
		return val, nil
	})
	if err != nil {
		return "", err
	}
	return v.(string), nil
}

// Invalidate drops a cached value, e.g. after the credential was rejected.
func (r *Resolver) Invalidate(ref string) {
	base, _, _ := strings.Cut(ref, "#")
	r.mu.Lock()
	delete(r.cache, base)
	r.mu.Unlock()
}

func (r *Resolver) lookup(ref string) (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.cache[ref]
	if !ok || !r.now().Before(c.expires) {
		return "", false
	}
	return c.value, true
}

// redact keeps references loggable without exposing anything sensitive; the
// reference itself is not secret, but keep it short.
func redact(ref string) string {
	if len(ref) > 80 {
		return ref[:77] + "..."
	}
	return ref
}

// Env resolves env://NAME from the process environment.
type Env struct{ Getenv func(string) string }

func (Env) Scheme() string { return "env" }

func (e Env) Resolve(_ context.Context, name string) (string, error) {
	get := e.Getenv
	if get == nil {
		get = os.Getenv
	}
	v := get(name)
	if v == "" {
		return "", fmt.Errorf("environment variable %s is not set", name)
	}
	return v, nil
}

// File resolves file:///abs/path, trimming one trailing newline (the usual
// shape of mounted Kubernetes secrets).
type File struct{}

func (File) Scheme() string { return "file" }

func (File) Resolve(_ context.Context, path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	v := strings.TrimSuffix(strings.TrimSuffix(string(data), "\n"), "\r")
	if v == "" {
		return "", fmt.Errorf("secret file %s is empty", path)
	}
	return v, nil
}

// UCSecretGetter is the subset of the Unity Catalog secrets API used here.
type UCSecretGetter interface {
	GetSecret(ctx context.Context, request catalog.GetSecretRequest) (*catalog.Secret, error)
}

// UnityCatalog resolves uc-secret://<catalog>.<schema>.<secret> from Unity
// Catalog secrets. The caller needs USE CATALOG, USE SCHEMA, and READ SECRET.
// Reads are rate limited and every access is recorded in the UC audit log.
type UnityCatalog struct {
	API     UCSecretGetter
	Limiter *rate.Limiter
}

func (UnityCatalog) Scheme() string { return "uc-secret" }

func (u UnityCatalog) Resolve(ctx context.Context, name string) (string, error) {
	parts := strings.Split(name, ".")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", fmt.Errorf("want uc-secret://<catalog>.<schema>.<secret>")
	}
	if u.API == nil {
		return "", fmt.Errorf("unity catalog secrets are not configured (no workspace client)")
	}
	if u.Limiter != nil {
		if err := u.Limiter.Wait(ctx); err != nil {
			return "", err
		}
	}
	s, err := u.API.GetSecret(ctx, catalog.GetSecretRequest{FullName: name, IncludeValue: true})
	switch {
	case errors.Is(err, apierr.ErrNotFound):
		return "", fmt.Errorf("unity catalog secret %s not found (or no USE SCHEMA/READ SECRET on %s.%s): %w", name, parts[0], parts[1], err)
	case errors.Is(err, apierr.ErrPermissionDenied):
		return "", fmt.Errorf("no access to unity catalog secret %s; grant READ SECRET on schema %s.%s: %w", name, parts[0], parts[1], err)
	case err != nil:
		return "", err
	}
	if s.EffectiveValue == "" {
		return "", fmt.Errorf("unity catalog secret %s has no value (READ SECRET is required to read values)", name)
	}
	return s.EffectiveValue, nil
}
