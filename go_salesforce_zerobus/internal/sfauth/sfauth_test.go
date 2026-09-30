package sfauth

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/tenant"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/testutil/fakeoauth"
)

func resolver(m map[string]string) Resolve {
	return func(_ context.Context, ref string) (string, error) { return m[ref], nil }
}

func TestOAuthAndSOAP(t *testing.T) {
	srv := fakeoauth.New()
	defer srv.Close()
	srv.AddClient("cid", "csecret", "00DA0000000000AAAA")
	srv.AddUser("u@example.com", "pw"+"tok", "00DB0000000000BBBB")
	httpc := NewHTTPClient()
	ctx := context.Background()

	oauth := NewAuthenticator(tenant.Salesforce{InstanceURL: srv.URL, Auth: tenant.Auth{
		Type: tenant.AuthOAuthClientCredentials, ClientID: "cid", ClientSecretRef: "env://S"}},
		httpc, resolver(map[string]string{"env://S": "csecret"}))
	c, err := oauth.Authenticate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if c.OrgID != "00DA0000000000AAAA" || c.InstanceURL != srv.URL || c.AccessToken == "" {
		t.Fatalf("oauth creds = %+v", c)
	}
	md := c.Metadata()
	if md.Get("tenantid")[0] != c.OrgID || md.Get("accesstoken")[0] != c.AccessToken {
		t.Errorf("metadata = %v", md)
	}

	soap := NewAuthenticator(tenant.Salesforce{InstanceURL: srv.URL, APIVersion: "62.0", Auth: tenant.Auth{
		Type: tenant.AuthSOAP, Username: "u@example.com", PasswordRef: "env://P", SecurityTokenRef: "env://T"}},
		httpc, resolver(map[string]string{"env://P": "pw", "env://T": "tok"}))
	c, err = soap.Authenticate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if c.OrgID != "00DB0000000000BBBB" || c.InstanceURL != srv.URL {
		t.Fatalf("soap creds = %+v", c)
	}

	// Rejected credentials are permanent; server errors are not.
	bad := NewAuthenticator(tenant.Salesforce{InstanceURL: srv.URL, Auth: tenant.Auth{
		Type: tenant.AuthOAuthClientCredentials, ClientID: "cid", ClientSecretRef: "env://S"}},
		httpc, resolver(map[string]string{"env://S": "wrong"}))
	if _, err := bad.Authenticate(ctx); err == nil || !IsPermanent(err) {
		t.Errorf("bad secret: err=%v permanent=%v", err, IsPermanent(err))
	}
	badSOAP := NewAuthenticator(tenant.Salesforce{InstanceURL: srv.URL, APIVersion: "62.0", Auth: tenant.Auth{
		Type: tenant.AuthSOAP, Username: "u@example.com", PasswordRef: "env://P"}},
		httpc, resolver(map[string]string{"env://P": "nope"}))
	if _, err := badSOAP.Authenticate(ctx); err == nil || !IsPermanent(err) {
		t.Errorf("bad soap: err=%v permanent=%v", err, IsPermanent(err))
	}
	srv.SetFailing(true)
	if _, err := oauth.Authenticate(ctx); err == nil || IsPermanent(err) {
		t.Errorf("503 should be transient: %v", err)
	}
}

func TestTokenSourceCachesAndCollapses(t *testing.T) {
	srv := fakeoauth.New()
	defer srv.Close()
	srv.AddClient("cid", "s", "00DA0000000000AAAA")
	a := NewAuthenticator(tenant.Salesforce{InstanceURL: srv.URL, Auth: tenant.Auth{
		Type: tenant.AuthOAuthClientCredentials, ClientID: "cid", ClientSecretRef: "env://S"}},
		NewHTTPClient(), resolver(map[string]string{"env://S": "s"}))
	ts := NewTokenSource(a, time.Hour, nil)
	now := time.Now()
	ts.now = func() time.Time { return now }

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := ts.Get(context.Background()); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if srv.Logins() != 1 {
		t.Fatalf("concurrent Gets should share one login, got %d", srv.Logins())
	}
	c1, _ := ts.Get(context.Background())

	// Invalidate forces a new login; invalidating a stale copy later is a no-op.
	ts.Invalidate(c1)
	c2, _ := ts.Get(context.Background())
	if c2 == c1 || srv.Logins() != 2 {
		t.Fatalf("invalidate did not refresh (logins=%d)", srv.Logins())
	}
	ts.Invalidate(c1)
	if c3, _ := ts.Get(context.Background()); c3 != c2 {
		t.Fatal("invalidating a stale session must not drop the current one")
	}

	// Proactive refresh near TTL.
	now = now.Add(56 * time.Minute)
	c4, _ := ts.Get(context.Background())
	if c4 == c2 || srv.Logins() != 3 {
		t.Fatalf("expected refresh before TTL (logins=%d)", srv.Logins())
	}
}
