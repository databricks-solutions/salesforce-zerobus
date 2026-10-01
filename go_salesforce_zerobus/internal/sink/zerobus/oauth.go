package zerobus

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	zb "github.com/databricks/zerobus-sdk/purego/zerobus"
	"golang.org/x/sync/singleflight"
)

// tokenProvider mints Unity Catalog OAuth tokens scoped to Zerobus writes on
// one table, and supplies them as stream headers.
//
// It replaces the SDK's built-in provider: purego v0.1.0 caps the token
// response at 4 KiB, which is too small for workspaces whose tokens embed the
// authorization_details claims (observed: 4,167 bytes), so every mint fails
// with "parse token response: unexpected EOF". The request is identical to the
// SDK's (same audience and authorization_details), so the token grants the
// same privileges. Remove once the SDK raises the limit.
type tokenProvider struct {
	tokenURL     string
	audience     string
	clientID     string
	clientSecret string
	http         *http.Client
	now          func() time.Time

	mu    sync.Mutex
	cache map[string]cachedToken
	group singleflight.Group
}

type cachedToken struct {
	token   string
	expires time.Time
}

var _ zb.HeadersProvider = (*tokenProvider)(nil)

// refreshBefore renews tokens this long before they expire.
const refreshBefore = 5 * time.Minute

func newTokenProvider(zerobusEndpoint, ucEndpoint, clientID, clientSecret string, httpc *http.Client) (*tokenProvider, error) {
	workspaceID, err := workspaceIDFromEndpoint(zerobusEndpoint)
	if err != nil {
		return nil, err
	}
	u, err := url.Parse(strings.TrimRight(strings.TrimSpace(ucEndpoint), "/"))
	if err != nil || u.Scheme != "https" || u.Host == "" {
		return nil, fmt.Errorf("unity catalog endpoint must be an https URL, got %q", ucEndpoint)
	}
	if httpc == nil {
		httpc = &http.Client{Timeout: 30 * time.Second}
	}
	return &tokenProvider{
		tokenURL:     u.String() + "/oidc/v1/token",
		audience:     "api://databricks/workspaces/" + workspaceID + "/zerobusDirectWriteApi",
		clientID:     clientID,
		clientSecret: clientSecret,
		http:         httpc,
		now:          func() time.Time { return time.Now().Round(0) }, // wall clock: survives host sleep
		cache:        map[string]cachedToken{},
	}, nil
}

// workspaceIDFromEndpoint returns the first DNS label of the Zerobus
// endpoint (<workspace-id>.zerobus.<region>.cloud.databricks.com).
func workspaceIDFromEndpoint(endpoint string) (string, error) {
	host := strings.TrimSpace(endpoint)
	if i := strings.Index(host, "://"); i >= 0 {
		host = host[i+3:]
	}
	host, _, _ = strings.Cut(host, "/")
	if h, _, ok := strings.Cut(host, ":"); ok {
		host = h
	}
	id, rest, ok := strings.Cut(host, ".")
	if !ok || id == "" || rest == "" {
		return "", fmt.Errorf("zerobus endpoint %q has no workspace ID subdomain", endpoint)
	}
	return id, nil
}

// GetHeaders implements zerobus.HeadersProvider.
func (p *tokenProvider) GetHeaders(ctx context.Context, table string) (map[string]string, error) {
	table = strings.TrimSpace(table)
	token, err := p.token(ctx, table)
	if err != nil {
		return nil, err
	}
	return map[string]string{
		"authorization":                   "Bearer " + token,
		"x-databricks-zerobus-table-name": table,
	}, nil
}

// Invalidate implements zerobus.HeadersProvider: the next GetHeaders mints
// a fresh token (called when the server rejects credentials).
func (p *tokenProvider) Invalidate(_ context.Context, table string) {
	p.mu.Lock()
	delete(p.cache, strings.TrimSpace(table))
	p.mu.Unlock()
}

func (p *tokenProvider) token(ctx context.Context, table string) (string, error) {
	p.mu.Lock()
	c, ok := p.cache[table]
	p.mu.Unlock()
	if ok && p.now().Before(c.expires.Add(-refreshBefore)) {
		return c.token, nil
	}
	v, err, _ := p.group.Do(table, func() (any, error) {
		tok, ttl, err := p.mint(ctx, table)
		if err != nil {
			return "", err
		}
		if ttl > 0 {
			p.mu.Lock()
			p.cache[table] = cachedToken{token: tok, expires: p.now().Add(ttl)}
			p.mu.Unlock()
		}
		return tok, nil
	})
	if err != nil {
		return "", err
	}
	return v.(string), nil
}

func (p *tokenProvider) mint(ctx context.Context, table string) (string, time.Duration, error) {
	parts := strings.Split(table, ".")
	if len(parts) != 3 {
		return "", 0, fmt.Errorf("table %q must be catalog.schema.table", table)
	}
	type detail struct {
		Type           string   `json:"type"`
		Privileges     []string `json:"privileges"`
		ObjectType     string   `json:"object_type"`
		ObjectFullPath string   `json:"object_full_path"`
		Operations     []string `json:"operations,omitempty"`
	}
	details, err := json.Marshal([]detail{
		{Type: "unity_catalog_privileges", Privileges: []string{"USE CATALOG"}, ObjectType: "CATALOG", ObjectFullPath: parts[0]},
		{Type: "unity_catalog_privileges", Privileges: []string{"USE SCHEMA"}, ObjectType: "SCHEMA", ObjectFullPath: parts[0] + "." + parts[1]},
		{Type: "unity_catalog_privileges", Privileges: []string{"SELECT", "MODIFY"}, ObjectType: "TABLE", ObjectFullPath: table, Operations: []string{"zerobuswrite"}},
	})
	if err != nil {
		return "", 0, err
	}
	form := url.Values{
		"grant_type":            {"client_credentials"},
		"scope":                 {"all-apis"},
		"resource":              {p.audience},
		"authorization_details": {string(details)},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(p.clientID, p.clientSecret)
	client := *p.http
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse } // never resend credentials
	resp, err := client.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("zerobus token request: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", 0, fmt.Errorf("reading zerobus token response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		var e struct {
			Error       string `json:"error"`
			Description string `json:"error_description"`
		}
		_ = json.Unmarshal(body, &e)
		if e.Error == "" {
			e.Description = strings.TrimSpace(string(body[:min(len(body), 300)]))
		}
		return "", 0, fmt.Errorf("zerobus token request for %s returned HTTP %d: %s %s", table, resp.StatusCode, e.Error, e.Description)
	}
	var tok struct {
		AccessToken string          `json:"access_token"`
		ExpiresIn   json.RawMessage `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &tok); err != nil {
		return "", 0, fmt.Errorf("parsing zerobus token response (%d bytes): %w", len(body), err)
	}
	if strings.TrimSpace(tok.AccessToken) == "" {
		return "", 0, fmt.Errorf("zerobus token response missing access_token")
	}
	var secs float64
	if err := json.Unmarshal(tok.ExpiresIn, &secs); err != nil {
		var s string
		if json.Unmarshal(tok.ExpiresIn, &s) == nil {
			fmt.Sscan(s, &secs)
		}
	}
	return tok.AccessToken, time.Duration(secs * float64(time.Second)), nil
}
