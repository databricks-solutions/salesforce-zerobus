// Package sfauth authenticates to Salesforce (OAuth 2.0 client credentials or
// SOAP login) and caches per-tenant sessions for the Pub/Sub API.
package sfauth

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/metadata"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/obs"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/tenant"
)

// Credentials is a Salesforce session.
type Credentials struct {
	AccessToken string
	InstanceURL string
	OrgID       string
	IssuedAt    time.Time
}

// Metadata returns the gRPC headers the Pub/Sub API requires.
func (c *Credentials) Metadata() metadata.MD {
	return metadata.Pairs("accesstoken", c.AccessToken, "instanceurl", c.InstanceURL, "tenantid", c.OrgID)
}

// Error is an authentication failure. Permanent errors (rejected
// credentials, bad configuration) will not succeed on retry without a config
// or secret change.
type Error struct {
	Permanent bool
	Err       error
}

func (e *Error) Error() string { return e.Err.Error() }
func (e *Error) Unwrap() error { return e.Err }

// IsPermanent reports whether err is a permanent authentication failure.
func IsPermanent(err error) bool {
	var e *Error
	return errors.As(err, &e) && e.Permanent
}

// Resolve resolves a secret reference.
type Resolve func(ctx context.Context, ref string) (string, error)

// Authenticator creates a new session.
type Authenticator interface {
	Authenticate(ctx context.Context) (*Credentials, error)
}

// NewHTTPClient returns the HTTP client shared by all tenants.
func NewHTTPClient() *http.Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns = 256
	tr.MaxIdleConnsPerHost = 4
	return &http.Client{Timeout: 30 * time.Second, Transport: tr}
}

// NewAuthenticator builds the authenticator for a tenant's configuration.
// Secrets are resolved on every login so rotated secrets are picked up.
func NewAuthenticator(sf tenant.Salesforce, httpc *http.Client, resolve Resolve) Authenticator {
	switch sf.Auth.Type {
	case tenant.AuthSOAP:
		return &soapAuth{sf: sf, http: httpc, resolve: resolve}
	default:
		return &oauthAuth{sf: sf, http: httpc, resolve: resolve}
	}
}

func resolveOr(ctx context.Context, resolve Resolve, literal, ref string) (string, error) {
	if literal != "" {
		return literal, nil
	}
	v, err := resolve(ctx, ref)
	if err != nil {
		// Missing secrets are configuration problems, not transient ones,
		// unless the secret store itself is unavailable.
		return "", &Error{Permanent: false, Err: err}
	}
	return v, nil
}

type oauthAuth struct {
	sf      tenant.Salesforce
	http    *http.Client
	resolve Resolve
}

func (a *oauthAuth) Authenticate(ctx context.Context) (*Credentials, error) {
	clientID, err := resolveOr(ctx, a.resolve, a.sf.Auth.ClientID, a.sf.Auth.ClientIDRef)
	if err != nil {
		return nil, err
	}
	secret, err := resolveOr(ctx, a.resolve, "", a.sf.Auth.ClientSecretRef)
	if err != nil {
		return nil, err
	}
	form := url.Values{"grant_type": {"client_credentials"}, "client_id": {clientID}, "client_secret": {secret}}
	body, status, err := a.do(ctx, http.MethodPost, a.sf.InstanceURL+"/services/oauth2/token",
		strings.NewReader(form.Encode()), "application/x-www-form-urlencoded", "")
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, httpError("OAuth token request", status, body)
	}
	var tok struct {
		AccessToken string `json:"access_token"`
		InstanceURL string `json:"instance_url"`
	}
	if err := json.Unmarshal(body, &tok); err != nil || tok.AccessToken == "" {
		return nil, &Error{Err: fmt.Errorf("OAuth token response missing access_token")}
	}
	instance := a.sf.InstanceURL
	if tok.InstanceURL != "" {
		instance = baseURL(tok.InstanceURL)
	}
	body, status, err = a.do(ctx, http.MethodGet, instance+"/services/oauth2/userinfo", nil, "", tok.AccessToken)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, httpError("OAuth userinfo request", status, body)
	}
	var info struct {
		OrganizationID string `json:"organization_id"`
	}
	if err := json.Unmarshal(body, &info); err != nil || info.OrganizationID == "" {
		return nil, &Error{Err: fmt.Errorf("userinfo response missing organization_id")}
	}
	return &Credentials{AccessToken: tok.AccessToken, InstanceURL: instance, OrgID: info.OrganizationID, IssuedAt: time.Now().Round(0)}, nil
}

func (a *oauthAuth) do(ctx context.Context, method, u string, body io.Reader, contentType, bearer string) ([]byte, int, error) {
	return doHTTP(ctx, a.http, method, u, body, contentType, bearer, nil)
}

type soapAuth struct {
	sf      tenant.Salesforce
	http    *http.Client
	resolve Resolve
}

func (a *soapAuth) Authenticate(ctx context.Context) (*Credentials, error) {
	username, err := resolveOr(ctx, a.resolve, a.sf.Auth.Username, a.sf.Auth.UsernameRef)
	if err != nil {
		return nil, err
	}
	password, err := resolveOr(ctx, a.resolve, "", a.sf.Auth.PasswordRef)
	if err != nil {
		return nil, err
	}
	if a.sf.Auth.SecurityTokenRef != "" {
		token, err := resolveOr(ctx, a.resolve, "", a.sf.Auth.SecurityTokenRef)
		if err != nil {
			return nil, err
		}
		password += token
	}
	envelope := `<?xml version="1.0" encoding="utf-8" ?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:urn="urn:partner.soap.sforce.com">
  <soapenv:Body><urn:login><urn:username>` + html.EscapeString(username) + `</urn:username><urn:password>` +
		html.EscapeString(password) + `</urn:password></urn:login></soapenv:Body>
</soapenv:Envelope>`
	u := a.sf.InstanceURL + "/services/Soap/u/" + a.sf.APIVersion + "/"
	body, status, err := doHTTP(ctx, a.http, http.MethodPost, u, bytes.NewBufferString(envelope), "text/xml; charset=utf-8", "",
		map[string]string{"SOAPAction": "login"})
	if err != nil {
		return nil, err
	}
	var env soapEnvelope
	if xerr := xml.Unmarshal(body, &env); xerr != nil {
		if status != http.StatusOK {
			return nil, httpError("SOAP login", status, body)
		}
		return nil, &Error{Err: fmt.Errorf("parsing SOAP login response: %w", xerr)}
	}
	if f := env.Body.Fault; f.Code != "" || f.String != "" {
		// INVALID_LOGIN, LOGIN_MUST_USE_SECURITY_TOKEN, etc.
		return nil, &Error{Permanent: true, Err: fmt.Errorf("SOAP login fault %s: %s", f.Code, f.String)}
	}
	r := env.Body.LoginResponse.Result
	if r.SessionID == "" || r.UserInfo.OrganizationID == "" {
		return nil, &Error{Err: fmt.Errorf("SOAP login response missing sessionId or organizationId")}
	}
	return &Credentials{AccessToken: r.SessionID, InstanceURL: baseURL(r.ServerURL), OrgID: r.UserInfo.OrganizationID, IssuedAt: time.Now().Round(0)}, nil
}

type soapEnvelope struct {
	Body struct {
		Fault struct {
			Code   string `xml:"faultcode"`
			String string `xml:"faultstring"`
		} `xml:"Fault"`
		LoginResponse struct {
			Result struct {
				SessionID string `xml:"sessionId"`
				ServerURL string `xml:"serverUrl"`
				UserInfo  struct {
					OrganizationID string `xml:"organizationId"`
				} `xml:"userInfo"`
			} `xml:"result"`
		} `xml:"loginResponse"`
	} `xml:"Body"`
}

func doHTTP(ctx context.Context, c *http.Client, method, u string, body io.Reader, contentType, bearer string, headers map[string]string) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, 0, &Error{Permanent: true, Err: err}
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := c.Do(req)
	if err != nil {
		return nil, 0, &Error{Err: fmt.Errorf("%s %s: %w", method, redactURL(u), err)}
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, resp.StatusCode, &Error{Err: err}
	}
	return data, resp.StatusCode, nil
}

// httpError classifies a non-200 response. 400/401/403 mean the credentials
// or client configuration were rejected; 429 and 5xx are transient.
func httpError(what string, status int, body []byte) error {
	msg := strings.TrimSpace(string(body))
	if len(msg) > 300 {
		msg = msg[:300]
	}
	permanent := status == http.StatusBadRequest || status == http.StatusUnauthorized || status == http.StatusForbidden
	return &Error{Permanent: permanent, Err: fmt.Errorf("%s returned %d: %s", what, status, msg)}
}

func baseURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return strings.TrimRight(raw, "/")
	}
	return u.Scheme + "://" + u.Host
}

func redactURL(u string) string {
	if i := strings.IndexByte(u, '?'); i >= 0 {
		return u[:i]
	}
	return u
}

// TokenSource caches one tenant's session. All of the tenant's
// subscriptions share it: concurrent refreshes collapse into one login, and
// logins across tenants are rate limited.
type TokenSource struct {
	auth    Authenticator
	ttl     time.Duration
	limiter *rate.Limiter
	now     func() time.Time

	mu    sync.Mutex
	cur   *Credentials
	group singleflight.Group
}

// NewTokenSource returns a TokenSource that refreshes sessions older than
// ttl (minus a margin). limiter may be nil.
func NewTokenSource(a Authenticator, ttl time.Duration, limiter *rate.Limiter) *TokenSource {
	if ttl <= 0 {
		ttl = time.Hour
	}
	// Wall-clock comparisons, so a session that aged out during host sleep is
	// not reused.
	return &TokenSource{auth: a, ttl: ttl, limiter: limiter, now: func() time.Time { return time.Now().Round(0) }}
}

// Get returns a valid session, logging in if needed.
func (t *TokenSource) Get(ctx context.Context) (*Credentials, error) {
	t.mu.Lock()
	cur := t.cur
	t.mu.Unlock()
	if cur != nil && t.now().Before(cur.IssuedAt.Add(t.ttl-min(5*time.Minute, t.ttl/10))) {
		return cur, nil
	}
	v, err, _ := t.group.Do("login", func() (any, error) {
		t.mu.Lock()
		if c := t.cur; c != nil && c != cur {
			t.mu.Unlock()
			return c, nil // another caller refreshed meanwhile
		}
		t.mu.Unlock()
		if t.limiter != nil {
			began := time.Now()
			if err := t.limiter.Wait(ctx); err != nil {
				return nil, err
			}
			obs.RateLimitWait.WithLabelValues("auth").Observe(time.Since(began).Seconds())
		}
		c, err := t.auth.Authenticate(ctx)
		if err != nil {
			obs.SFAuth.WithLabelValues("error").Inc()
			return nil, err
		}
		obs.SFAuth.WithLabelValues("ok").Inc()
		t.mu.Lock()
		t.cur = c
		t.mu.Unlock()
		return c, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*Credentials), nil
}

// Invalidate discards stale (e.g. after UNAUTHENTICATED) so the next Get
// logs in again. It is a no-op if the session was already replaced.
func (t *TokenSource) Invalidate(stale *Credentials) {
	t.mu.Lock()
	if t.cur == stale {
		t.cur = nil
	}
	t.mu.Unlock()
}
