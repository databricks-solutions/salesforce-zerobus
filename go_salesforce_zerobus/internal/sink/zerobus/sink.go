// Package zerobus is the Zerobus sink: per-table pools of stream slots, each
// with one batching writer goroutine and an ack tracker that turns Zerobus
// offsets back into per-subscription progress.
package zerobus

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sort"
	"sync"
	"time"

	zb "github.com/databricks/zerobus-sdk/purego/zerobus"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/backoff"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/shard"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/sink"
)

// Stream is the subset of *zerobus.Stream the sink uses.
type Stream interface {
	IngestRecordsOffsetContext(ctx context.Context, records [][]byte) (int64, error)
	Close() error
	IsClosed() bool
	ID() string
}

// Factory opens streams for a table.
type Factory interface {
	Open(ctx context.Context, table string, cb zb.AckCallback) (Stream, error)
	Close() error
}

// Options tunes the sink.
type Options struct {
	// SlotsForTable sizes a new table pool from its subscription count.
	SlotsForTable   func(subs int) int
	BatchMaxRecords int
	BatchMaxBytes   int
	BatchLinger     time.Duration
	// OpenTimeout bounds each stream open.
	OpenTimeout time.Duration
	// Reopen is the backoff between failed opens.
	Reopen backoff.Policy
	// EnsureTable is called (until it succeeds) before a table's first
	// stream opens, e.g. to create or migrate the Delta table.
	EnsureTable func(ctx context.Context, table string) error
	Logger      *slog.Logger
}

func (o *Options) defaults() {
	if o.SlotsForTable == nil {
		o.SlotsForTable = func(int) int { return 1 }
	}
	if o.BatchMaxRecords <= 0 {
		o.BatchMaxRecords = 500
	}
	if o.BatchMaxBytes <= 0 {
		o.BatchMaxBytes = 4 << 20
	}
	if o.BatchLinger <= 0 {
		o.BatchLinger = 20 * time.Millisecond
	}
	if o.OpenTimeout <= 0 {
		o.OpenTimeout = time.Minute
	}
	if o.Reopen.Base <= 0 {
		o.Reopen = backoff.Policy{Base: 2 * time.Second, Max: 2 * time.Minute}
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
}

// Sink implements sink.Sink.
type Sink struct {
	opts    Options
	factory Factory

	ctx    context.Context // cancelled only after Close's drain deadline
	cancel context.CancelFunc

	mu      sync.Mutex
	tables  map[string]*tablePool
	closing bool
	wg      sync.WaitGroup
}

var _ sink.Sink = (*Sink)(nil)

// New creates a sink over factory.
func New(factory Factory, opts Options) *Sink {
	opts.defaults()
	ctx, cancel := context.WithCancel(context.Background())
	return &Sink{opts: opts, factory: factory, ctx: ctx, cancel: cancel, tables: map[string]*tablePool{}}
}

type tablePool struct {
	name  string
	slots []*slot

	ensureMu sync.Mutex
	ensured  bool
}

func (p *tablePool) ensure(ctx context.Context, fn func(context.Context, string) error) error {
	if fn == nil {
		return nil
	}
	p.ensureMu.Lock()
	defer p.ensureMu.Unlock()
	if p.ensured {
		return nil
	}
	if err := fn(ctx, p.name); err != nil {
		return err
	}
	p.ensured = true
	return nil
}

// Prepare creates pools (and starts their writers) for new tables.
func (s *Sink) Prepare(_ context.Context, tables map[string]int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return sink.ErrClosed
	}
	names := make([]string, 0, len(tables))
	for t := range tables {
		names = append(names, t)
	}
	sort.Strings(names)
	for _, t := range names {
		s.poolLocked(t, tables[t])
	}
	return nil
}

func (s *Sink) poolLocked(table string, subs int) *tablePool {
	if p, ok := s.tables[table]; ok {
		return p
	}
	n := max(1, s.opts.SlotsForTable(subs))
	p := &tablePool{name: table}
	for i := 0; i < n; i++ {
		p.slots = append(p.slots, newSlot(s, p, i))
	}
	for _, sl := range p.slots { // start only once the slot list is complete
		s.wg.Add(1)
		go sl.run()
	}
	s.tables[table] = p
	s.opts.Logger.Info("Zerobus table pool created", "table", table, "slots", n, "subscriptions", subs)
	return p
}

// Open pins ref to a slot of its table.
func (s *Sink) Open(ref *sink.SubscriptionRef) (sink.Writer, error) {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return nil, sink.ErrClosed
	}
	p := s.poolLocked(ref.Table, 1)
	s.mu.Unlock()
	h := fnv.New64a()
	h.Write([]byte(ref.Key.String()))
	sl := p.slots[shard.Jump(h.Sum64(), int32(len(p.slots)))]
	sl.register(ref)
	return &writer{slot: sl, ref: ref}, nil
}

// Close drains every slot, flushes and closes streams (applying their final
// acks), then closes the SDK connections. Writers still running at the ctx
// deadline are abandoned.
func (s *Sink) Close(ctx context.Context) error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return nil
	}
	s.closing = true
	var slots []*slot
	for _, p := range s.tables {
		slots = append(slots, p.slots...)
	}
	s.mu.Unlock()
	for _, sl := range slots {
		sl.beginClose()
	}
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	var err error
	select {
	case <-done:
	case <-ctx.Done():
		err = fmt.Errorf("sink close: %w (some rows may be redelivered)", ctx.Err())
	}
	s.cancel()
	return errors.Join(err, s.factory.Close())
}

// Health returns the state of every slot.
func (s *Sink) Health() []sink.StreamHealth {
	s.mu.Lock()
	var slots []*slot
	for _, p := range s.tables {
		slots = append(slots, p.slots...)
	}
	s.mu.Unlock()
	out := make([]sink.StreamHealth, 0, len(slots))
	for _, sl := range slots {
		out = append(out, sl.health())
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Table != out[j].Table {
			return out[i].Table < out[j].Table
		}
		return out[i].Slot < out[j].Slot
	})
	return out
}

// Ready reports whether every slot has opened at least once.
func (s *Sink) Ready() bool {
	for _, h := range s.Health() {
		if h.State == stateConnecting && h.Failures == 0 {
			return false
		}
	}
	return true
}

type writer struct {
	slot *slot
	ref  *sink.SubscriptionRef
	once sync.Once
}

func (w *writer) Submit(ctx context.Context, r *sink.Record) error {
	return w.slot.submit(ctx, w.ref, r)
}

func (w *writer) Close() { w.once.Do(func() { w.slot.unregister(w.ref) }) }
