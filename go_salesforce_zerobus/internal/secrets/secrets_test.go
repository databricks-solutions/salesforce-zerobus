package secrets

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/databricks/databricks-sdk-go/apierr"
	"github.com/databricks/databricks-sdk-go/service/catalog"
)

// fakeUC serves Unity Catalog secrets by full name.
type fakeUC struct {
	calls  atomic.Int32
	values map[string]string
	denied map[string]bool
}

func (f *fakeUC) GetSecret(_ context.Context, req catalog.GetSecretRequest) (*catalog.Secret, error) {
	f.calls.Add(1)
	if !req.IncludeValue {
		return nil, errors.New("include_value must be set to read the value")
	}
	if f.denied[req.FullName] {
		return nil, fmt.Errorf("PERMISSION_DENIED: %w", apierr.ErrPermissionDenied)
	}
	v, ok := f.values[req.FullName]
	if !ok {
		return nil, fmt.Errorf("SECRET_DOES_NOT_EXIST: %w", apierr.ErrNotFound)
	}
	parts := strings.Split(req.FullName, ".")
	return &catalog.Secret{CatalogName: parts[0], SchemaName: parts[1], Name: parts[2], FullName: req.FullName, EffectiveValue: v}, nil
}

func TestResolver(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pw")
	os.WriteFile(path, []byte("s3cret\n"), 0o600)
	uc := &fakeUC{
		values: map[string]string{
			"main.sfzb_secrets.acme_prod": `{"client_id":"3MVG9EXAMPLE","client_secret":"uc-s3cret"}`,
			"main.sfzb_secrets.plain":     "just-a-value",
		},
		denied: map[string]bool{"main.sfzb_secrets.locked": true},
	}
	r := NewResolver(time.Minute,
		Env{Getenv: func(k string) string { return map[string]string{"A": "alpha"}[k] }},
		File{},
		UnityCatalog{API: uc},
	)
	now := time.Unix(0, 0)
	r.now = func() time.Time { return now }
	ctx := context.Background()

	for ref, want := range map[string]string{
		"env://A":                             "alpha",
		"file://" + path:                      "s3cret",
		"uc-secret://main.sfzb_secrets.plain": "just-a-value",
		"uc-secret://main.sfzb_secrets.acme_prod#client_secret": "uc-s3cret",
	} {
		if got, err := r.Resolve(ctx, ref); err != nil || got != want {
			t.Errorf("Resolve(%s) = %q, %v; want %q", ref, got, err, want)
		}
	}

	// Different #fields of one secret share a single cached read.
	calls := uc.calls.Load()
	if v, err := r.Resolve(ctx, "uc-secret://main.sfzb_secrets.acme_prod#client_id"); err != nil || v != "3MVG9EXAMPLE" {
		t.Errorf("client_id = %q, %v", v, err)
	}
	if uc.calls.Load() != calls {
		t.Errorf("expected cached value, calls %d -> %d", calls, uc.calls.Load())
	}
	now = now.Add(2 * time.Minute)
	r.Resolve(ctx, "uc-secret://main.sfzb_secrets.acme_prod#client_secret")
	if uc.calls.Load() != calls+1 {
		t.Errorf("expected refetch after TTL, calls = %d", uc.calls.Load())
	}

	os.WriteFile(filepath.Join(dir, "tenant.json"), []byte(`{"client_id":"3MVG","client_secret":"s3"}`), 0o600)
	if v, err := r.Resolve(ctx, "file://"+filepath.Join(dir, "tenant.json")+"#client_secret"); err != nil || v != "s3" {
		t.Errorf("json field = %q, %v", v, err)
	}

	for ref, want := range map[string]string{
		"uc-secret://main.sfzb_secrets.missing":             "not found",
		"uc-secret://main.sfzb_secrets.locked":              "grant READ SECRET on schema main.sfzb_secrets",
		"uc-secret://main.sfzb_secrets":                     "want uc-secret://<catalog>.<schema>.<secret>",
		"uc-secret://main.sfzb_secrets.acme_prod#nope":      `has no key "nope"`,
		"uc-secret://main.sfzb_secrets.plain#field":         "not a JSON object",
		"databricks-secrets://salesforce-zerobus/acme-prod": `unknown scheme "databricks-secrets"`,
		"env://MISSING": "not set",
		"nope":          "want scheme://path",
	} {
		if _, err := r.Resolve(ctx, ref); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("Resolve(%q) err = %v, want containing %q", ref, err, want)
		}
	}
	if got := r.Schemes(); len(got) != 3 || got[2] != "uc-secret" {
		t.Errorf("Schemes() = %v", got)
	}
}

func TestUnityCatalogWithoutWorkspace(t *testing.T) {
	if _, err := (UnityCatalog{}).Resolve(context.Background(), "a.b.c"); err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("err = %v", err)
	}
}
