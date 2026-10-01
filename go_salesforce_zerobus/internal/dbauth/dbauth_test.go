package dbauth

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/databricks/databricks-sdk-go/config/experimental/auth"
	"golang.org/x/oauth2"
)

func hasMonotonic(t time.Time) bool { return strings.Contains(t.String(), " m=") }

func TestWallClockStripsMonotonic(t *testing.T) {
	src := auth.TokenSourceFn(func(context.Context) (*oauth2.Token, error) {
		return &oauth2.Token{AccessToken: "x", Expiry: time.Now().Add(time.Hour)}, nil
	})
	if !hasMonotonic(time.Now().Add(time.Hour)) {
		t.Skip("platform clock has no monotonic reading")
	}
	tok, err := WallClock(src).Token(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if hasMonotonic(tok.Expiry) {
		t.Fatalf("expiry still carries a monotonic reading: %v", tok.Expiry)
	}
	if !hasMonotonic(time.Now()) || hasMonotonic(Now()) {
		t.Fatal("Now() must be wall-clock only")
	}
}

func TestM2MRequest(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, secret, _ := r.BasicAuth()
		r.ParseForm()
		if r.URL.Path != "/oidc/v1/token" || id != "sp" || secret != "s3cret" || r.Form.Get("grant_type") != "client_credentials" || r.Form.Get("scope") != "all-apis" {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"access_token": "tok", "token_type": "Bearer", "expires_in": 3600})
	}))
	defer srv.Close()
	tok, err := M2M(srv.URL+"/", "sp", "s3cret", srv.Client()).Token(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if tok.AccessToken != "tok" || hasMonotonic(tok.Expiry) || time.Until(tok.Expiry) < 59*time.Minute {
		t.Fatalf("token = %+v", tok)
	}
}
