package obs

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Admin serves health, readiness, metrics, and debug endpoints.
type Admin struct {
	Addr string
	// Live reports liveness (process making progress). Never tie it to a
	// single tenant: one broken org must not restart the replica.
	Live func() error
	// Ready reports readiness (dependencies initialized).
	Ready func() error
	// Subscriptions returns per-subscription status, optionally filtered.
	Subscriptions func(state, tenant, table string) any
	// Streams returns Zerobus stream slot health.
	Streams func() any
	Pprof   bool
	Logger  *slog.Logger

	listener net.Listener
}

// Listen binds the address (so callers learn the port before serving).
func (a *Admin) Listen() (string, error) {
	l, err := net.Listen("tcp", a.Addr)
	if err != nil {
		return "", err
	}
	a.listener = l
	return l.Addr().String(), nil
}

// Handler returns the admin mux.
func (a *Admin) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", check(a.Live))
	mux.HandleFunc("GET /readyz", check(a.Ready))
	mux.Handle("GET /metrics", promhttp.HandlerFor(Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /debug/subscriptions", func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		all := a.Subscriptions(q.Get("state"), q.Get("tenant"), q.Get("table"))
		writeJSON(w, paginate(all, q.Get("limit"), q.Get("offset")))
	})
	mux.HandleFunc("GET /debug/streams", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, a.Streams()) })
	if a.Pprof {
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	}
	return mux
}

// Serve serves until ctx is done.
func (a *Admin) Serve(ctx context.Context) error {
	if a.listener == nil {
		if _, err := a.Listen(); err != nil {
			return err
		}
	}
	srv := &http.Server{Handler: a.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()
	a.Logger.Info("Admin server listening", "addr", a.listener.Addr().String())
	if err := srv.Serve(a.listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func check(fn func() error) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if fn != nil {
			if err := fn(); err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
		}
		w.Write([]byte("ok\n"))
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}

type page struct {
	Total  int `json:"total"`
	Offset int `json:"offset"`
	Items  any `json:"items"`
}

// paginate slices v if it is a slice of JSON-able items (via reflection-free
// round trip through []any).
func paginate(v any, limitS, offsetS string) any {
	raw, err := json.Marshal(v)
	if err != nil {
		return v
	}
	var items []json.RawMessage
	if json.Unmarshal(raw, &items) != nil {
		return v
	}
	limit, _ := strconv.Atoi(limitS)
	offset, _ := strconv.Atoi(offsetS)
	if limit <= 0 {
		limit = 500
	}
	offset = min(max(offset, 0), len(items))
	end := min(offset+limit, len(items))
	return page{Total: len(items), Offset: offset, Items: items[offset:end]}
}
