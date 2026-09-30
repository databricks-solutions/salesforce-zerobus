package supervisor

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/backoff"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/checkpoint"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/obs"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/pubsub"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/sink"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/tenant"
)

// Runner states.
const (
	StatePending = "pending" // waiting for its startup slot
	StateRunning = "running"
	StateBackoff = "backoff" // retrying after a transient error
	StateFailed  = "failed"  // needs attention (credentials, config); retried slowly
)

var allStates = []string{StatePending, StateRunning, StateBackoff, StateFailed}

// Policy holds retry delays per error class.
type Policy struct {
	Transient, Quota, Sink backoff.Policy
	Failed                 time.Duration // permanent auth / config errors
	HealthyAfter           time.Duration // a run this long resets the backoff attempt counter
}

// DefaultPolicy returns production retry delays.
func DefaultPolicy() Policy {
	return Policy{
		Transient:    backoff.Policy{Base: time.Second, Max: time.Minute},
		Quota:        backoff.Policy{Base: 30 * time.Second, Max: 10 * time.Minute},
		Sink:         backoff.Policy{Base: 2 * time.Second, Max: 2 * time.Minute},
		Failed:       15 * time.Minute,
		HealthyAfter: 5 * time.Minute,
	}
}

type runner struct {
	key           tenant.SubKey
	spec          tenant.SubscriptionSpec
	hash          string
	sub           *pubsub.Subscription
	wm            *checkpoint.Watermark
	writer        sink.Writer
	stats         *pubsub.Stats
	checkpointOrg string
	policy        Policy
	log           *slog.Logger

	cancel context.CancelFunc
	done   chan struct{}

	mu          sync.Mutex
	state       string
	lastErr     string
	lastClass   string
	lastErrAt   time.Time
	nextRetryAt time.Time
	restarts    int
	startedAt   time.Time
	errLoggedAt time.Time
}

func (r *runner) run(ctx context.Context, delay time.Duration) {
	defer close(r.done)
	if delay > 0 {
		if backoff.SleepFor(ctx, delay) != nil {
			return
		}
	}
	gen := r.wm.Restart()
	start, err := r.initialStart(ctx)
	if err != nil && ctx.Err() != nil {
		return
	}
	attempt := 0
	for ctx.Err() == nil {
		r.setRunning()
		began := time.Now()
		err := r.safeRun(ctx, gen, start)
		if stopping(ctx) {
			return
		}
		if time.Since(began) >= r.policy.HealthyAfter {
			attempt = 0
		}
		class := pubsub.Classify(err)
		obs.SubscriptionRestart.WithLabelValues(class.String()).Inc()

		var delay time.Duration
		state := StateBackoff
		switch class {
		case pubsub.ClassTransient:
			start = r.startFrom(r.wm.ResumePoint())
			delay = r.policy.Transient.Delay(attempt)
		case pubsub.ClassAuth:
			// The token was invalidated; retry immediately once.
			start = r.startFrom(r.wm.ResumePoint())
			if attempt > 0 {
				delay = r.policy.Transient.Delay(attempt)
			}
		case pubsub.ClassReplayExpired:
			obs.ReplayFallback.WithLabelValues(string(r.spec.OnReplayExpired)).Inc()
			r.log.Error("Stored replay ID has expired; falling back to preset (events may have been missed during the outage)",
				"preset", r.spec.OnReplayExpired, "replay_id", hex.EncodeToString(r.wm.ResumePoint()))
			start = pubsub.Start{Preset: r.spec.OnReplayExpired}
		case pubsub.ClassQuota:
			start = r.startFrom(r.wm.ResumePoint())
			delay = r.policy.Quota.Delay(attempt)
		case pubsub.ClassSinkReset, pubsub.ClassSinkUnavailable:
			// In-flight rows may be lost: new generation, resume from acked.
			gen = r.wm.Restart()
			start = r.startFrom(r.wm.Acked())
			delay = r.policy.Sink.Delay(attempt)
		case pubsub.ClassPermanentAuth, pubsub.ClassConfig:
			start = r.startFrom(r.wm.ResumePoint())
			delay = r.policy.Failed
			state = StateFailed
		case pubsub.ClassCanceled:
			return
		}
		attempt++
		r.recordError(class, err, state, delay)
		if backoff.SleepFor(ctx, delay) != nil {
			return
		}
	}
}

// initialStart resolves the first start position, discarding a stored
// checkpoint that belongs to a different org (e.g. a refreshed sandbox).
func (r *runner) initialStart(ctx context.Context) (pubsub.Start, error) {
	acked := r.wm.Acked()
	if len(acked) == 0 {
		return pubsub.Start{Preset: r.spec.ReplayDefault}, nil
	}
	if r.checkpointOrg != "" {
		creds, err := r.sub.Tokens.Get(ctx)
		if err == nil && !sameOrg(creds.OrgID, r.checkpointOrg) {
			obs.CheckpointOrgMismatch.Inc()
			r.log.Warn("Ignoring stored checkpoint from a different org", "stored_org", r.checkpointOrg, "org", creds.OrgID)
			r.wm.ClearPosition()
			return pubsub.Start{Preset: r.spec.ReplayDefault}, nil
		}
		if err != nil {
			return r.startFrom(acked), err // Run will surface the auth error
		}
	}
	return r.startFrom(acked), nil
}

func (r *runner) startFrom(replay []byte) pubsub.Start {
	if len(replay) == 0 {
		return pubsub.Start{Preset: r.spec.ReplayDefault}
	}
	return pubsub.Start{ReplayID: replay}
}

func (r *runner) safeRun(ctx context.Context, gen uint64, start pubsub.Start) (err error) {
	defer func() {
		if p := recover(); p != nil {
			obs.Panics.Inc()
			r.log.Error("Subscription panicked; restarting", "panic", p, "stack", string(debug.Stack()))
			err = &pubsub.Error{Class: pubsub.ClassTransient, Err: fmt.Errorf("panic: %v", p)}
		}
	}()
	r.log.Info("Subscribing", "start", start.String(), "gen", gen)
	return r.sub.Run(ctx, gen, start)
}

func (r *runner) recordError(class pubsub.Class, err error, state string, delay time.Duration) {
	r.mu.Lock()
	changed := r.lastClass != class.String() || r.state != state
	r.restarts++
	r.lastClass = class.String()
	r.lastErr = err.Error()
	r.lastErrAt = time.Now()
	r.nextRetryAt = time.Now().Add(delay)
	r.state = state
	// Log on state change, then at most every 5 minutes per subscription.
	logIt := changed || time.Since(r.errLoggedAt) > 5*time.Minute
	if logIt {
		r.errLoggedAt = time.Now()
	}
	r.mu.Unlock()
	if logIt {
		level := slog.LevelWarn
		if state == StateFailed {
			level = slog.LevelError
		}
		r.log.Log(context.Background(), level, "Subscription interrupted", "class", class.String(), "error", err, "retry_in", delay.Round(time.Millisecond))
	}
}

func (r *runner) setRunning() {
	r.mu.Lock()
	r.state = StateRunning
	r.startedAt = time.Now()
	r.mu.Unlock()
}

// State returns the runner state.
func (r *runner) State() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state
}

// SubStatus is the externally visible status of one subscription.
type SubStatus struct {
	Tenant          string    `json:"tenant"`
	Topic           string    `json:"topic"`
	Table           string    `json:"table"`
	OrgID           string    `json:"org_id,omitempty"`
	State           string    `json:"state"`
	Gen             uint64    `json:"gen"`
	LastErrorClass  string    `json:"last_error_class,omitempty"`
	LastError       string    `json:"last_error,omitempty"`
	LastErrorAt     time.Time `json:"last_error_at,omitzero"`
	NextRetryAt     time.Time `json:"next_retry_at,omitzero"`
	Restarts        int       `json:"restarts"`
	RunningSince    time.Time `json:"running_since,omitzero"`
	Received        int64     `json:"received"`
	Submitted       int64     `json:"submitted"`
	Acked           int64     `json:"acked"`
	Duplicates      int64     `json:"duplicates"`
	DecodeErrors    int64     `json:"decode_errors"`
	Unacked         int64     `json:"unacked"`
	Pending         int64     `json:"pending_requested"`
	LastEventAt     time.Time `json:"last_event_at,omitzero"`
	LastAckAt       time.Time `json:"last_ack_at,omitzero"`
	AckedReplay     string    `json:"acked_replay_id,omitempty"`
	CommittedReplay string    `json:"committed_replay_id,omitempty"`
}

func (r *runner) status() SubStatus {
	wm := r.wm.Status()
	r.mu.Lock()
	defer r.mu.Unlock()
	st := SubStatus{
		Tenant: string(r.key.Tenant), Topic: r.key.Topic, Table: r.spec.Table, OrgID: wm.OrgID, State: r.state, Gen: wm.Gen,
		LastErrorClass: r.lastClass, LastError: r.lastErr, LastErrorAt: r.lastErrAt, Restarts: r.restarts,
		Received: r.stats.Received.Load(), Submitted: r.stats.Submitted.Load(), Acked: wm.TotalAcked,
		Duplicates: r.stats.Duplicates.Load(), DecodeErrors: r.stats.DecodeErrs.Load(), Unacked: wm.Unacked,
		Pending: r.stats.Pending.Load(), LastAckAt: wm.LastAckAt,
		AckedReplay: hex.EncodeToString(wm.Acked), CommittedReplay: hex.EncodeToString(wm.Committed),
	}
	if r.state == StateRunning {
		st.RunningSince = r.startedAt
	} else {
		st.NextRetryAt = r.nextRetryAt
	}
	if ns := r.stats.LastEventAt.Load(); ns > 0 {
		st.LastEventAt = time.Unix(0, ns)
	}
	return st
}

// stopping reports whether ctx is done or past its deadline (gRPC can
// report a deadline slightly before the context's own timer fires).
func stopping(ctx context.Context) bool {
	if ctx.Err() != nil {
		return true
	}
	dl, ok := ctx.Deadline()
	return ok && !time.Now().Before(dl)
}

func sameOrg(a, b string) bool {
	if len(a) >= 15 && len(b) >= 15 {
		return a[:15] == b[:15]
	}
	return a == b
}
