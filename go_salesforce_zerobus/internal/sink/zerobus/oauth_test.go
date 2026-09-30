package zerobus

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTokenProviderLargeResponseAndCaching(t *testing.T) {
	var mints atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/oidc/v1/token" {
			http.NotFound(w, r)
			return
		}
		id, secret, _ := r.BasicAuth()
		r.ParseForm()
		if id != "sp" || secret != "s3cret" || r.Form.Get("grant_type") != "client_credentials" {
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]string{"error": "invalid_client", "error_description": "bad creds"})
			return
		}
		if got := r.Form.Get("resource"); got != "api://databricks/workspaces/1234567890123456/zerobusDirectWriteApi" {
			t.Errorf("resource = %q", got)
		}
		var details []map[string]any
		json.Unmarshal([]byte(r.Form.Get("authorization_details")), &details)
		if len(details) != 3 || details[2]["object_full_path"] != "cat.sch.tbl" {
			t.Errorf("authorization_details = %v", details)
		}
		mints.Add(1)
		// Real tokens with embedded authorization_details exceed 4 KiB.
		json.NewEncoder(w).Encode(map[string]any{"access_token": strings.Repeat("x", 6000), "expires_in": 3600, "token_type": "Bearer"})
	}))
	defer srv.Close()

	p, err := newTokenProvider("https://1234567890123456.zerobus.us-west-2.cloud.databricks.com", srv.URL, "sp", "s3cret", srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(0, 0)
	p.now = func() time.Time { return now }

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			h, err := p.GetHeaders(context.Background(), " cat.sch.tbl ")
			if err != nil {
				t.Error(err)
				return
			}
			if len(h["authorization"]) != len("Bearer ")+6000 || h["x-databricks-zerobus-table-name"] != "cat.sch.tbl" {
				t.Errorf("headers = %d bytes auth, table %q", len(h["authorization"]), h["x-databricks-zerobus-table-name"])
			}
		}()
	}
	wg.Wait()
	if mints.Load() != 1 {
		t.Fatalf("concurrent GetHeaders should share one mint, got %d", mints.Load())
	}

	now = now.Add(56 * time.Minute) // within refreshBefore of expiry
	p.GetHeaders(context.Background(), "cat.sch.tbl")
	if mints.Load() != 2 {
		t.Fatalf("expected refresh near expiry, mints = %d", mints.Load())
	}
	p.Invalidate(context.Background(), "cat.sch.tbl")
	p.GetHeaders(context.Background(), "cat.sch.tbl")
	if mints.Load() != 3 {
		t.Fatalf("expected re-mint after Invalidate, mints = %d", mints.Load())
	}

	bad, _ := newTokenProvider("1234567890123456.zerobus.us-west-2.cloud.databricks.com", srv.URL, "sp", "wrong", srv.Client())
	if _, err := bad.GetHeaders(context.Background(), "cat.sch.tbl"); err == nil || !strings.Contains(err.Error(), "HTTP 401: invalid_client bad creds") {
		t.Fatalf("err = %v", err)
	}
}

func TestWorkspaceIDFromEndpoint(t *testing.T) {
	for in, want := range map[string]string{
		"1234567890123456.zerobus.us-west-2.cloud.databricks.com":          "1234567890123456",
		"https://1234567890123456.zerobus.us-west-2.cloud.databricks.com/": "1234567890123456",
		"123.zerobus.example.com:443":                                      "123",
	} {
		if got, err := workspaceIDFromEndpoint(in); err != nil || got != want {
			t.Errorf("%s -> %q, %v", in, got, err)
		}
	}
	if _, err := workspaceIDFromEndpoint("localhost"); err == nil {
		t.Error("single-label host should fail")
	}
}
