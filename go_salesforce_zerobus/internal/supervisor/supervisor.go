// Package supervisor runs one isolated runner per owned subscription and
// reconciles them against tenant snapshots. A failing org never affects
// others: each runner retries on its own schedule by error class.
package supervisor

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/checkpoint"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/obs"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/pubsub"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/sfauth"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/sink"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/tenant"
)

// Deps are the supervisor's collaborators.
type Deps struct {
	Shared      *pubsub.Shared
	Sink        sink.Sink
	Registry    *checkpoint.Registry
	Store       checkpoint.Store
	HTTP        *http.Client
	Resolve     sfauth.Resolve
	AuthLimiter *rate.Limiter
	// StartupSpread caps the random delay spreading out first starts.
	StartupSpread time.Duration
	// Policy overrides retry delays (tests).
	Policy *Policy
	Logger *slog.Logger
}

// Supervisor owns the runners.
type Supervisor struct {
	deps   Deps
	policy Policy

	mu      sync.Mutex
	runners map[tenant.SubKey]*runner
	tokens  map[tenant.Key]*tokenEntry
	wg      sync.WaitGroup

	heartbeat  atomic.Int64
	reconciled atomic.Bool

	final []SubStatus // statuses captured when the supervisor stopped
}

type tokenEntry struct {
	hash string
	ts   *sfauth.TokenSource
}

// New creates a supervisor.
func New(deps Deps) *Supervisor {
	p := DefaultPolicy()
	if deps.Policy != nil {
		p = *deps.Policy
	}
	return &Supervisor{deps: deps, policy: p, runners: map[tenant.SubKey]*runner{}, tokens: map[tenant.Key]*tokenEntry{}}
}

// Run reconciles each snapshot until ctx is done, then stops every runner.
// It returns only on shutdown; per-subscription failures never end it.
func (s *Supervisor) Run(ctx context.Context, snaps <-chan tenant.Snapshot) error {
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	s.heartbeat.Store(time.Now().UnixNano())
	for {
		select {
		case <-ctx.Done():
			s.stopAll()
			return nil
		case snap := <-snaps:
			if err := s.Reconcile(ctx, snap); err != nil && ctx.Err() == nil {
				s.deps.Logger.Error("Reconcile failed; will retry on the next change", "error", err)
			}
		case <-tick.C:
		}
		s.heartbeat.Store(time.Now().UnixNano())
		s.updateGauges()
	}
}

// Heartbeat returns when the supervisor loop last ran.
func (s *Supervisor) Heartbeat() time.Time { return time.Unix(0, s.heartbeat.Load()) }

// Reconciled reports whether the first snapshot has been applied.
func (s *Supervisor) Reconciled() bool { return s.reconciled.Load() }

// Reconcile makes the running set match snap (already filtered to this
// shard).
func (s *Supervisor) Reconcile(ctx context.Context, snap tenant.Snapshot) error {
	type want struct {
		t    tenant.Tenant
		spec tenant.SubscriptionSpec
		hash string
	}
	desired := map[tenant.SubKey]want{}
	for _, t := range snap.Tenants {
		if !t.Enabled {
			continue
		}
		for _, sp := range t.Subscriptions {
			desired[sp.Key] = want{t, sp, t.Hash(sp)}
		}
	}

	// Stop removed or changed subscriptions (in parallel).
	s.mu.Lock()
	var stopping []*runner
	for k, r := range s.runners {
		if w, ok := desired[k]; !ok || w.hash != r.hash {
			stopping = append(stopping, r)
			delete(s.runners, k)
		}
	}
	s.mu.Unlock()
	for _, r := range stopping {
		r.cancel()
	}
	for _, r := range stopping {
		<-r.done
		r.writer.Close()
		if _, still := desired[r.key]; !still {
			s.deps.Registry.Release(checkpoint.KeyOf(r.key))
		}
	}

	// Collect new subscriptions.
	s.mu.Lock()
	var starting []want
	for k, w := range desired {
		if _, ok := s.runners[k]; !ok {
			starting = append(starting, w)
		}
	}
	s.pruneTokensLocked(snap)
	s.mu.Unlock()
	sort.Slice(starting, func(i, j int) bool { return starting[i].spec.Key.String() < starting[j].spec.Key.String() })

	if err := s.deps.Sink.Prepare(ctx, snap.Tables()); err != nil {
		return err
	}
	keys := make([]checkpoint.Key, 0, len(starting))
	for _, w := range starting {
		keys = append(keys, checkpoint.KeyOf(w.spec.Key))
	}
	var stored map[checkpoint.Key]checkpoint.Checkpoint
	if len(keys) > 0 {
		var err error
		if stored, err = s.deps.Store.LoadMany(ctx, keys); err != nil {
			return fmt.Errorf("loading %d checkpoints: %w", len(keys), err)
		}
	}

	first := !s.reconciled.Load()
	spread := time.Duration(0)
	if first && s.deps.StartupSpread > 0 {
		spread = min(s.deps.StartupSpread, time.Duration(len(starting))*50*time.Millisecond)
	}
	for _, w := range starting {
		cp, ok := stored[checkpoint.KeyOf(w.spec.Key)]
		var delay time.Duration
		if spread > 0 {
			delay = rand.N(spread)
		}
		if err := s.start(ctx, w.t, w.spec, w.hash, cp, ok, delay); err != nil {
			s.deps.Logger.Error("Could not start subscription", "tenant", w.spec.Key.Tenant, "topic", w.spec.Key.Topic, "error", err)
		}
	}
	s.reconciled.Store(true)
	s.mu.Lock()
	running := len(s.runners)
	s.mu.Unlock()
	obs.OwnedTenants.Set(float64(len(snap.Tenants)))
	s.deps.Logger.Info("Reconciled subscriptions", "version", snap.Version, "running", running,
		"started", len(starting), "stopped", len(stopping))
	return nil
}

func (s *Supervisor) tokenSource(t tenant.Tenant) *sfauth.TokenSource {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := fmt.Sprintf("%+v", t.Salesforce)
	if e, ok := s.tokens[t.Key]; ok && e.hash == h {
		return e.ts
	}
	auth := sfauth.NewAuthenticator(t.Salesforce, s.deps.HTTP, s.deps.Resolve)
	ts := sfauth.NewTokenSource(auth, t.Salesforce.TokenTTL, s.deps.AuthLimiter)
	s.tokens[t.Key] = &tokenEntry{hash: h, ts: ts}
	return ts
}

func (s *Supervisor) pruneTokensLocked(snap tenant.Snapshot) {
	keep := map[tenant.Key]bool{}
	for _, t := range snap.Tenants {
		keep[t.Key] = true
	}
	for k := range s.tokens {
		if !keep[k] {
			delete(s.tokens, k)
		}
	}
}

func (s *Supervisor) start(ctx context.Context, t tenant.Tenant, spec tenant.SubscriptionSpec, hash string,
	cp checkpoint.Checkpoint, haveCP bool, delay time.Duration) error {
	key := checkpoint.KeyOf(spec.Key)
	wm := s.deps.Registry.Register(key, spec.Table, "", cp.ReplayID)
	ref := &sink.SubscriptionRef{Key: spec.Key, Table: spec.Table, Acker: wm}
	w, err := s.deps.Sink.Open(ref)
	if err != nil {
		return err
	}
	log := s.deps.Logger.With("tenant", string(spec.Key.Tenant), "topic", spec.Key.Topic, "table", spec.Table)
	stats := &pubsub.Stats{}
	sub := &pubsub.Subscription{
		Spec: spec, ExpectedOrgID: t.ExpectedOrgID, Tokens: s.tokenSource(t), Writer: w, Watermark: wm,
		Dedup: pubsub.NewDedupCache(spec.DedupSize), Stats: stats, Shared: s.deps.Shared, Logger: log,
	}
	rctx, cancel := context.WithCancel(ctx)
	r := &runner{
		key: spec.Key, spec: spec, hash: hash, sub: sub, wm: wm, writer: w, stats: stats,
		cancel: cancel, done: make(chan struct{}), policy: s.policy, log: log, state: StatePending,
	}
	if haveCP {
		r.checkpointOrg = cp.OrgID
	}
	s.mu.Lock()
	s.runners[spec.Key] = r
	s.mu.Unlock()
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		r.run(rctx, delay)
	}()
	return nil
}

func (s *Supervisor) stopAll() {
	s.mu.Lock()
	runners := make([]*runner, 0, len(s.runners))
	for _, r := range s.runners {
		runners = append(runners, r)
	}
	s.runners = map[tenant.SubKey]*runner{}
	s.mu.Unlock()
	for _, r := range runners {
		r.cancel()
	}
	s.wg.Wait()
	final := make([]SubStatus, 0, len(runners))
	for _, r := range runners {
		r.writer.Close()
		final = append(final, r.status())
	}
	sortStatuses(final)
	s.mu.Lock()
	s.final = final
	s.mu.Unlock()
}

// FinalStatuses returns the statuses captured when Run stopped (empty
// while running).
func (s *Supervisor) FinalStatuses() []SubStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]SubStatus(nil), s.final...)
}

func sortStatuses(out []SubStatus) {
	sort.Slice(out, func(i, j int) bool {
		if out[i].Tenant != out[j].Tenant {
			return out[i].Tenant < out[j].Tenant
		}
		return out[i].Topic < out[j].Topic
	})
}

func (s *Supervisor) updateGauges() {
	counts := map[string]int{}
	for _, st := range allStates {
		counts[st] = 0
	}
	s.mu.Lock()
	for _, r := range s.runners {
		counts[r.State()]++
	}
	s.mu.Unlock()
	for st, n := range counts {
		obs.Subscriptions.WithLabelValues(st).Set(float64(n))
	}
}

// Statuses returns the status of every runner, sorted by key.
func (s *Supervisor) Statuses() []SubStatus {
	s.mu.Lock()
	runners := make([]*runner, 0, len(s.runners))
	for _, r := range s.runners {
		runners = append(runners, r)
	}
	s.mu.Unlock()
	out := make([]SubStatus, 0, len(runners))
	for _, r := range runners {
		out = append(out, r.status())
	}
	sortStatuses(out)
	return out
}
