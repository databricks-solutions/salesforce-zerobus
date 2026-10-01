// Package dbauth authenticates the Databricks SDK as the service principal
// (OAuth machine-to-machine) with token expiry tracked on the wall clock.
//
// The SDK's default M2M cache compares expiry using Go's monotonic clock,
// which does not advance while a laptop sleeps or a container VM is
// suspended. After such a pause the SDK can keep presenting an expired token
// ("Invalid Token") until the monotonic clock catches up, up to an hour. This
// token source strips the monotonic reading from every expiry, so expiry
// checks (ours and the SDK's cache around it) use real time.
package dbauth

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/databricks/databricks-sdk-go/config"
	"github.com/databricks/databricks-sdk-go/config/experimental/auth"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

// M2M returns a token source for the workspace OIDC token endpoint using the
// client credentials grant. httpc may be nil.
func M2M(host, clientID, clientSecret string, httpc *http.Client) auth.TokenSource {
	cc := &clientcredentials.Config{
		ClientID:     clientID,
		ClientSecret: clientSecret,
		TokenURL:     strings.TrimRight(host, "/") + "/oidc/v1/token",
		Scopes:       []string{"all-apis"},
		AuthStyle:    oauth2.AuthStyleInHeader,
	}
	return WallClock(auth.TokenSourceFn(func(ctx context.Context) (*oauth2.Token, error) {
		if httpc != nil {
			ctx = context.WithValue(ctx, oauth2.HTTPClient, httpc)
		}
		return cc.Token(ctx)
	}))
}

// WallClock wraps ts so returned tokens carry a wall-clock-only expiry.
func WallClock(ts auth.TokenSource) auth.TokenSource {
	return auth.TokenSourceFn(func(ctx context.Context) (*oauth2.Token, error) {
		t, err := ts.Token(ctx)
		if err != nil || t == nil {
			return t, err
		}
		c := *t
		c.Expiry = c.Expiry.Round(0) // drop the monotonic reading
		return &c, nil
	})
}

// Strategy returns an SDK credentials strategy for the service principal.
// The SDK caches and proactively refreshes tokens from it.
func Strategy(host, clientID, clientSecret string) config.CredentialsStrategy {
	return config.NewTokenSourceStrategy("oauth-m2m", M2M(host, clientID, clientSecret, nil))
}

// Now returns the current wall-clock time (no monotonic reading). Use it for
// any comparison against a credential expiry.
func Now() time.Time { return time.Now().Round(0) }
