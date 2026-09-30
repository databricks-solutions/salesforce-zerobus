package checkpoint

import (
	"bytes"
	"sync"
	"time"
)

// Watermark is the in-memory progress of one subscription. It implements
// sink.Acker. All methods are safe for concurrent use and never block, so
// they can be called from the SDK's ack dispatcher.
type Watermark struct {
	key    Key
	notify chan struct{} // credit freed (acks arrived)
	reset  chan error    // sink slot failed for the current generation

	mu         sync.Mutex
	table      string
	orgID      string
	gen        uint64
	acked      []byte // last durably acked replay ID
	committed  []byte // last persisted replay ID
	submitted  []byte // last replay ID handed to the sink in this generation
	unacked    int64  // submitted but not yet acked, this generation
	ackedDelta int64  // acked events not yet persisted
	seq        uint64 // bumps on every progress change
	dirty      bool
	dirtySince time.Time
	totalAcked int64
	lastAckAt  time.Time
	released   bool
}

func newWatermark(key Key, table, orgID string, initial []byte) *Watermark {
	return &Watermark{
		key:       key,
		table:     table,
		orgID:     orgID,
		acked:     clone(initial),
		committed: clone(initial),
		notify:    make(chan struct{}, 1),
		reset:     make(chan error, 1),
	}
}

// Key returns the checkpoint key.
func (w *Watermark) Key() Key { return w.key }

// Notify fires (coalesced) whenever acks free up credit.
func (w *Watermark) Notify() <-chan struct{} { return w.notify }

// ResetCh receives when the sink lost in-flight records of this generation.
func (w *Watermark) ResetCh() <-chan error { return w.reset }

// Gen returns the current generation.
func (w *Watermark) Gen() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.gen
}

// SetOrg records the authenticated org ID (stored with the checkpoint).
func (w *Watermark) SetOrg(orgID string) {
	w.mu.Lock()
	w.orgID = orgID
	w.mu.Unlock()
}

// Restart begins a new generation: in-flight records of earlier generations
// are forgotten (their acks will be ignored) and the subscription must resume
// from Acked(). It returns the new generation.
func (w *Watermark) Restart() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.gen++
	w.submitted = nil
	w.unacked = 0
	select {
	case <-w.reset:
	default:
	}
	return w.gen
}

// ClearPosition forgets the resume position (e.g. a stored checkpoint from a
// different org). The next commit overwrites the stored row.
func (w *Watermark) ClearPosition() {
	w.mu.Lock()
	w.acked, w.submitted = nil, nil
	w.mu.Unlock()
}

// Acked returns the last durably acked replay ID (or the initial checkpoint).
func (w *Watermark) Acked() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return clone(w.acked)
}

// ResumePoint returns where to resubscribe after a Salesforce-side error in
// the current generation: the last submitted replay ID (records handed to
// the sink will still be acked), falling back to the acked position.
func (w *Watermark) ResumePoint() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.submitted != nil {
		return clone(w.submitted)
	}
	return clone(w.acked)
}

// BeginSubmit reserves one unacked slot before a record is handed to the
// sink (so an ack can never be counted before its submit). It returns false if
// gen is stale.
func (w *Watermark) BeginSubmit(gen uint64) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if gen != w.gen {
		return false
	}
	w.unacked++
	return true
}

// EndSubmit completes BeginSubmit. On failure the slot is released.
func (w *Watermark) EndSubmit(gen uint64, replayID []byte, ok bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if gen != w.gen {
		return
	}
	if ok {
		w.submitted = clone(replayID)
	} else if w.unacked > 0 {
		w.unacked--
	}
}

// Advance records that n records of generation gen, the last with replayID,
// are durable. Implements sink.Acker.
func (w *Watermark) Advance(gen uint64, replayID []byte, n int) {
	w.mu.Lock()
	if gen != w.gen {
		w.mu.Unlock()
		return
	}
	w.acked = clone(replayID)
	w.unacked = max(0, w.unacked-int64(n))
	w.ackedDelta += int64(n)
	w.totalAcked += int64(n)
	w.lastAckAt = time.Now()
	w.markDirtyLocked()
	w.mu.Unlock()
	select {
	case w.notify <- struct{}{}:
	default:
	}
}

// AdvanceIdle moves the position to a keepalive's latest replay ID when
// nothing is in flight, so quiet subscriptions do not resume from a replay
// ID that ages out of Salesforce's retention window.
func (w *Watermark) AdvanceIdle(gen uint64, replayID []byte) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if gen != w.gen || w.unacked != 0 || len(replayID) == 0 || bytes.Equal(replayID, w.acked) {
		return false
	}
	w.acked = clone(replayID)
	w.submitted = clone(replayID)
	w.markDirtyLocked()
	return true
}

// Reset reports that the sink lost in-flight records of generation gen.
// Implements sink.Acker.
func (w *Watermark) Reset(gen uint64, err error) {
	w.mu.Lock()
	current := gen == w.gen
	w.mu.Unlock()
	if !current {
		return
	}
	select {
	case w.reset <- err:
	default:
	}
}

// Unacked returns records submitted but not yet acked in this generation.
func (w *Watermark) Unacked() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.unacked
}

func (w *Watermark) markDirtyLocked() {
	w.seq++
	if !w.dirty {
		w.dirty = true
		w.dirtySince = time.Now()
	}
}

// Status is a point-in-time view for diagnostics.
type Status struct {
	Gen        uint64
	OrgID      string
	Table      string
	Acked      []byte
	Committed  []byte
	Unacked    int64
	TotalAcked int64
	LastAckAt  time.Time
	Dirty      bool
}

// Status returns a snapshot of the watermark.
func (w *Watermark) Status() Status {
	w.mu.Lock()
	defer w.mu.Unlock()
	return Status{
		Gen: w.gen, OrgID: w.orgID, Table: w.table, Acked: clone(w.acked), Committed: clone(w.committed),
		Unacked: w.unacked, TotalAcked: w.totalAcked, LastAckAt: w.lastAckAt, Dirty: w.dirty,
	}
}

// snapshot returns the checkpoint to persist if there is uncommitted
// progress.
func (w *Watermark) snapshot() (cp Checkpoint, seq uint64, since time.Time, ok bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.dirty || w.orgID == "" || len(w.acked) == 0 {
		return Checkpoint{}, 0, time.Time{}, false
	}
	return Checkpoint{Key: w.key, OrgID: w.orgID, Table: w.table, ReplayID: clone(w.acked), EventsAcked: w.ackedDelta},
		w.seq, w.dirtySince, true
}

// markCommitted records a successful save of snapshot seq.
func (w *Watermark) markCommitted(seq uint64, cp Checkpoint) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.ackedDelta -= cp.EventsAcked
	w.committed = cp.ReplayID
	if w.seq == seq {
		w.dirty = false
	}
}

func (w *Watermark) setTable(table string) {
	w.mu.Lock()
	w.table = table
	w.released = false
	w.mu.Unlock()
}

func (w *Watermark) release() {
	w.mu.Lock()
	w.released = true
	w.mu.Unlock()
}

func (w *Watermark) removable() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.released && !w.dirty
}

func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte(nil), b...)
}
