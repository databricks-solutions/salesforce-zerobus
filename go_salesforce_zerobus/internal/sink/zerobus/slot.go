package zerobus

import (
	"context"
	"sync"
	"time"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/obs"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/sink"
)

const (
	stateConnecting = "connecting"
	stateReady      = "ready"
	stateBroken     = "broken"
	stateClosed     = "closed"
)

type item struct {
	ref *sink.SubscriptionRef
	rec *sink.Record
}

// slot is one Zerobus stream with a single writer goroutine. Subscriptions
// are pinned to a slot, so their records share one ordered offset sequence.
type slot struct {
	sink *Sink
	pool *tablePool
	idx  int
	in   chan item

	closing   chan struct{}
	closeOnce sync.Once
	failCh    chan error

	mu         sync.Mutex
	state      string
	streamID   string
	tracker    *ackTracker
	refs       map[*sink.SubscriptionRef]struct{}
	active     map[*sink.SubscriptionRef]uint64 // refs with records on the current stream -> latest gen
	failures   int
	lastErr    string
	lastErrAt  time.Time
	everOpened bool
}

func newSlot(s *Sink, p *tablePool, idx int) *slot {
	return &slot{
		sink: s, pool: p, idx: idx,
		in:      make(chan item, 2*s.opts.BatchMaxRecords),
		closing: make(chan struct{}),
		failCh:  make(chan error, 1),
		state:   stateConnecting,
		refs:    map[*sink.SubscriptionRef]struct{}{},
		active:  map[*sink.SubscriptionRef]uint64{},
	}
}

func (s *slot) register(ref *sink.SubscriptionRef) {
	s.mu.Lock()
	s.refs[ref] = struct{}{}
	s.mu.Unlock()
}

func (s *slot) unregister(ref *sink.SubscriptionRef) {
	s.mu.Lock()
	delete(s.refs, ref)
	s.mu.Unlock()
}

func (s *slot) beginClose() { s.closeOnce.Do(func() { close(s.closing) }) }

func (s *slot) submit(ctx context.Context, ref *sink.SubscriptionRef, r *sink.Record) error {
	s.mu.Lock()
	state := s.state
	if state == stateReady {
		s.active[ref] = r.Gen
	}
	s.mu.Unlock()
	switch state {
	case stateReady:
	case stateClosed:
		return sink.ErrClosed
	default:
		return sink.ErrUnavailable
	}
	select {
	case s.in <- item{ref: ref, rec: r}:
		obs.EventsSubmitted.WithLabelValues(s.pool.name).Inc()
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closing:
		return sink.ErrClosed
	}
}

func (s *slot) run() {
	defer s.sink.wg.Done()
	opts := &s.sink.opts
	for attempt := 0; ; {
		select {
		case <-s.closing:
			s.setState(stateClosed)
			return
		default:
		}
		st, tr, err := s.open()
		if err != nil {
			s.recordFailure(err)
			obs.ZerobusStreamFailures.WithLabelValues(s.pool.name).Inc()
			opts.Logger.Warn("Zerobus stream open failed", "table", s.pool.name, "slot", s.idx, "attempt", attempt+1, "error", err)
			t := time.NewTimer(opts.Reopen.Delay(attempt))
			select {
			case <-s.closing:
				t.Stop()
				s.setState(stateClosed)
				return
			case <-t.C:
			}
			attempt++
			continue
		}
		attempt = 0
		err = s.pump(st, tr)
		if err == nil {
			return // closed gracefully
		}
		s.fail(st, tr, err)
	}
}

func (s *slot) open() (Stream, *ackTracker, error) {
	s.setState(stateConnecting)
	ctx, cancel := context.WithTimeout(s.sink.ctx, s.sink.opts.OpenTimeout)
	defer cancel()
	if err := s.pool.ensure(ctx, s.sink.opts.EnsureTable); err != nil {
		return nil, nil, err
	}
	tr := newAckTracker(s.pool.name)
	began := time.Now()
	st, err := s.sink.factory.Open(ctx, s.pool.name, &callback{slot: s, tr: tr})
	if err != nil {
		return nil, nil, err
	}
	obs.StreamOpenSeconds.WithLabelValues("zerobus").Observe(time.Since(began).Seconds())
	s.mu.Lock()
	s.state, s.streamID, s.tracker, s.everOpened = stateReady, st.ID(), tr, true
	s.active = map[*sink.SubscriptionRef]uint64{}
	s.mu.Unlock()
	// Drop a failure signal left over from the previous stream.
	select {
	case <-s.failCh:
	default:
	}
	s.updateGauge()
	s.sink.opts.Logger.Info("Zerobus stream ready", "table", s.pool.name, "slot", s.idx, "stream_id", st.ID())
	return st, tr, nil
}

// pump batches queued records into the stream until it fails (returning
// the error) or the sink closes (returning nil after a final flush).
func (s *slot) pump(st Stream, tr *ackTracker) error {
	opts := &s.sink.opts
	var carry *item
	for {
		var first item
		if carry != nil {
			first, carry = *carry, nil
		} else {
			select {
			case first = <-s.in:
			case err := <-s.failCh:
				return err
			case <-s.closing:
				return s.drainAndClose(st, tr)
			}
		}
		batch := []item{first}
		size := len(first.rec.Payload)
		linger := time.NewTimer(opts.BatchLinger)
	collect:
		for len(batch) < opts.BatchMaxRecords {
			select {
			case it := <-s.in:
				if size+len(it.rec.Payload) > opts.BatchMaxBytes {
					carry = &it
					break collect
				}
				batch = append(batch, it)
				size += len(it.rec.Payload)
			case <-linger.C:
				break collect
			case err := <-s.failCh:
				linger.Stop()
				return err
			case <-s.closing:
				break collect
			}
		}
		linger.Stop()
		if err := s.ingest(st, tr, batch); err != nil {
			return err
		}
	}
}

func (s *slot) ingest(st Stream, tr *ackTracker, batch []item) error {
	payloads := make([][]byte, len(batch))
	var oldest time.Time
	type key struct {
		ref *sink.SubscriptionRef
		gen uint64
	}
	idx := map[key]int{}
	var updates []update
	for i, it := range batch {
		payloads[i] = it.rec.Payload
		if oldest.IsZero() || (!it.rec.ReceivedAt.IsZero() && it.rec.ReceivedAt.Before(oldest)) {
			oldest = it.rec.ReceivedAt
		}
		k := key{it.ref, it.rec.Gen}
		if j, ok := idx[k]; ok {
			updates[j].replayID = it.rec.ReplayID
			updates[j].n++
		} else {
			idx[k] = len(updates)
			updates = append(updates, update{ref: it.ref, gen: it.rec.Gen, replayID: it.rec.ReplayID, n: 1})
		}
	}
	off, err := st.IngestRecordsOffsetContext(s.sink.ctx, payloads)
	if err != nil {
		return err
	}
	obs.ZerobusBatchRecords.WithLabelValues(s.pool.name).Observe(float64(len(batch)))
	tr.append(pending{offset: off, n: len(batch), oldest: oldest, updates: updates})
	return nil
}

// drainAndClose ingests what is queued, then flushes. After a successful
// flush every appended batch is durable, so progress is applied directly
// instead of waiting for asynchronous ack callbacks.
func (s *slot) drainAndClose(st Stream, tr *ackTracker) error {
	for {
		var batch []item
	drain:
		for len(batch) < s.sink.opts.BatchMaxRecords {
			select {
			case it := <-s.in:
				batch = append(batch, it)
			default:
				break drain
			}
		}
		if len(batch) == 0 {
			break
		}
		if err := s.ingest(st, tr, batch); err != nil {
			s.sink.opts.Logger.Warn("Zerobus ingest failed during shutdown; unflushed rows will be redelivered", "table", s.pool.name, "error", err)
			st.Close()
			s.setState(stateClosed)
			return nil
		}
	}
	if err := st.Close(); err != nil {
		s.sink.opts.Logger.Warn("Zerobus flush on shutdown failed; unacked rows will be redelivered", "table", s.pool.name, "slot", s.idx, "error", err)
	} else {
		tr.ackAll()
	}
	s.setState(stateClosed)
	return nil
}

// fail tears down a broken stream: pending progress is discarded, queued
// records are dropped, and every subscription with records on the stream is
// reset so it resumes from its last acked position.
func (s *slot) fail(st Stream, tr *ackTracker, err error) {
	tr.discard()
	go st.Close() // already terminal; do not block the slot on teardown
	for {
		select {
		case <-s.in:
			continue
		default:
		}
		break
	}
	s.mu.Lock()
	active := s.active
	s.active = map[*sink.SubscriptionRef]uint64{}
	s.mu.Unlock()
	s.recordFailure(err)
	obs.ZerobusStreamFailures.WithLabelValues(s.pool.name).Inc()
	s.sink.opts.Logger.Warn("Zerobus stream failed; resetting pinned subscriptions",
		"table", s.pool.name, "slot", s.idx, "subscriptions", len(active), "error", err)
	for ref, gen := range active {
		ref.Acker.Reset(gen, err)
	}
}

func (s *slot) signalFail(tr *ackTracker, err error) {
	s.mu.Lock()
	current := s.tracker == tr && s.state == stateReady
	s.mu.Unlock()
	if !current {
		return
	}
	select {
	case s.failCh <- err:
	default:
	}
}

func (s *slot) recordFailure(err error) {
	s.mu.Lock()
	s.state = stateBroken
	s.failures++
	s.lastErr = err.Error()
	s.lastErrAt = time.Now()
	s.mu.Unlock()
	s.updateGauge()
}

func (s *slot) setState(state string) {
	s.mu.Lock()
	s.state = state
	s.mu.Unlock()
	s.updateGauge()
}

func (s *slot) updateGauge() {
	counts := map[string]float64{stateConnecting: 0, stateReady: 0, stateBroken: 0, stateClosed: 0}
	for _, sl := range s.pool.slots {
		sl.mu.Lock()
		counts[sl.state]++
		sl.mu.Unlock()
	}
	for state, n := range counts {
		obs.ZerobusStreams.WithLabelValues(s.pool.name, state).Set(n)
	}
}

func (s *slot) health() sink.StreamHealth {
	s.mu.Lock()
	defer s.mu.Unlock()
	return sink.StreamHealth{
		Table: s.pool.name, Slot: s.idx, State: s.state, StreamID: s.streamID, Subscriptions: len(s.refs),
		Failures: s.failures, LastError: s.lastErr, LastErrorAt: s.lastErrAt,
	}
}

// callback adapts SDK ack notifications to one stream instance's tracker.
type callback struct {
	slot *slot
	tr   *ackTracker
}

func (c *callback) OnAck(offset int64) { c.tr.ackThrough(offset) }

func (c *callback) OnError(_ int64, err error) { c.slot.signalFail(c.tr, err) }
