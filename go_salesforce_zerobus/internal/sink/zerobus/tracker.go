package zerobus

import (
	"sync"
	"time"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/obs"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/sink"
)

// pending is one ingested batch awaiting its ack.
type pending struct {
	offset  int64
	n       int
	oldest  time.Time
	updates []update
}

// update is the progress one subscription makes when a batch is acked: the
// last replay ID it contributed and how many records.
type update struct {
	ref      *sink.SubscriptionRef
	gen      uint64
	replayID []byte
	n        int
}

// ackTracker maps Zerobus offsets back to subscription progress for one
// stream instance. Offsets are dense and acked in order, so a FIFO suffices.
// An ack may arrive before its batch is appended (the SDK dispatches on its
// own goroutine), hence ackedThrough. Updates are applied under the lock so
// progress for a subscription is always applied in offset order.
type ackTracker struct {
	table string

	mu           sync.Mutex
	q            []pending
	ackedThrough int64
	lastAppended int64
	closed       bool
}

func newAckTracker(table string) *ackTracker {
	return &ackTracker{table: table, ackedThrough: -1, lastAppended: -1}
}

func (t *ackTracker) append(p pending) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return
	}
	t.q = append(t.q, p)
	t.lastAppended = p.offset
	t.drainLocked()
}

// ackThrough marks every offset <= off durable.
func (t *ackTracker) ackThrough(off int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return
	}
	if off > t.ackedThrough {
		t.ackedThrough = off
	}
	t.drainLocked()
}

// ackAll marks everything appended so far durable (after a successful
// flush).
func (t *ackTracker) ackAll() {
	t.mu.Lock()
	last := t.lastAppended
	t.mu.Unlock()
	t.ackThrough(last)
}

// discard drops all pending batches; later acks are ignored.
func (t *ackTracker) discard() {
	t.mu.Lock()
	t.closed = true
	t.q = nil
	t.mu.Unlock()
}

func (t *ackTracker) drainLocked() {
	i := 0
	now := time.Now()
	for ; i < len(t.q) && t.q[i].offset <= t.ackedThrough; i++ {
		p := t.q[i]
		for _, u := range p.updates {
			u.ref.Acker.Advance(u.gen, u.replayID, u.n)
		}
		obs.EventsAcked.WithLabelValues(t.table).Add(float64(p.n))
		if !p.oldest.IsZero() {
			obs.EventE2ESeconds.WithLabelValues(t.table).Observe(now.Sub(p.oldest).Seconds())
		}
		t.q[i] = pending{}
	}
	t.q = t.q[i:]
}

func (t *ackTracker) len() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.q)
}
