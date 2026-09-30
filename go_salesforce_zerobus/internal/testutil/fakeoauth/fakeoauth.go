// Package fakeoauth is an in-process Salesforce login server (OAuth client
// credentials, userinfo, and SOAP login) for tests and load generation. One
// server can host any number of orgs.
package fakeoauth

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
)

// Server is a fake Salesforce login endpoint.
type Server struct {
	*httptest.Server

	mu      sync.Mutex
	clients map[string]client // client_id -> client
	users   map[string]client // username -> user (password in secret)
	tokens  map[string]string // access token -> org ID
	logins  atomic.Int64
	failing atomic.Bool
}

type client struct{ secret, orgID string }

// New starts a server.
func New() *Server {
	s := &Server{clients: map[string]client{}, users: map[string]client{}, tokens: map[string]string{}}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /services/oauth2/token", s.token)
	mux.HandleFunc("GET /services/oauth2/userinfo", s.userinfo)
	mux.HandleFunc("POST /services/Soap/u/{version}/", s.soap)
	s.Server = httptest.NewServer(mux)
	return s
}

// AddClient registers an OAuth client for orgID.
func (s *Server) AddClient(clientID, secret, orgID string) {
	s.mu.Lock()
	s.clients[clientID] = client{secret, orgID}
	s.mu.Unlock()
}

// AddUser registers a SOAP user for orgID. password must include any
// security token suffix.
func (s *Server) AddUser(username, password, orgID string) {
	s.mu.Lock()
	s.users[username] = client{password, orgID}
	s.mu.Unlock()
}

// OrgForToken returns the org of a valid access token.
func (s *Server) OrgForToken(token string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	org, ok := s.tokens[token]
	return org, ok
}

// RevokeAll invalidates every issued token (simulates session expiry).
func (s *Server) RevokeAll() {
	s.mu.Lock()
	s.tokens = map[string]string{}
	s.mu.Unlock()
}

// Logins returns the number of successful logins.
func (s *Server) Logins() int64 { return s.logins.Load() }

// SetFailing makes logins return 503.
func (s *Server) SetFailing(v bool) { s.failing.Store(v) }

func (s *Server) issue(orgID string) string {
	n := s.logins.Add(1)
	tok := fmt.Sprintf("00D!tok-%s-%d", orgID, n)
	s.mu.Lock()
	s.tokens[tok] = orgID
	s.mu.Unlock()
	return tok
}

func (s *Server) token(w http.ResponseWriter, r *http.Request) {
	if s.failing.Load() {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	r.ParseForm()
	s.mu.Lock()
	c, ok := s.clients[r.Form.Get("client_id")]
	s.mu.Unlock()
	if !ok || c.secret != r.Form.Get("client_secret") || r.Form.Get("grant_type") != "client_credentials" {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "invalid_client", "error_description": "invalid client credentials"})
		return
	}
	json.NewEncoder(w).Encode(map[string]string{"access_token": s.issue(c.orgID), "instance_url": s.URL, "token_type": "Bearer"})
}

func (s *Server) userinfo(w http.ResponseWriter, r *http.Request) {
	org, ok := s.OrgForToken(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	if !ok {
		http.Error(w, "Bad_OAuth_Token", http.StatusForbidden)
		return
	}
	json.NewEncoder(w).Encode(map[string]string{"organization_id": org})
}

func (s *Server) soap(w http.ResponseWriter, r *http.Request) {
	if s.failing.Load() {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	var env struct {
		Body struct {
			Login struct {
				Username string `xml:"username"`
				Password string `xml:"password"`
			} `xml:"login"`
		} `xml:"Body"`
	}
	if err := xml.NewDecoder(r.Body).Decode(&env); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	u, ok := s.users[env.Body.Login.Username]
	s.mu.Unlock()
	w.Header().Set("Content-Type", "text/xml")
	if !ok || u.secret != env.Body.Login.Password {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, `<?xml version="1.0"?><soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"><soapenv:Body><soapenv:Fault><faultcode>INVALID_LOGIN</faultcode><faultstring>INVALID_LOGIN: Invalid username, password, security token; or user locked out.</faultstring></soapenv:Fault></soapenv:Body></soapenv:Envelope>`)
		return
	}
	fmt.Fprintf(w, `<?xml version="1.0"?><soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"><soapenv:Body><loginResponse><result><serverUrl>%s/services/Soap/u/62.0/%s</serverUrl><sessionId>%s</sessionId><userInfo><organizationId>%s</organizationId></userInfo></result></loginResponse></soapenv:Body></soapenv:Envelope>`,
		s.URL, u.orgID, s.issue(u.orgID), u.orgID)
}
