package zerobus

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/backoff"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/sink"
	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/tenant"
)

// recAcker records progress and asserts it only moves forward.
type recAcker struct {
	t      *testing.T
	mu     sync.Mutex
	last   uint64 // last replay seq acked
	acked  int
	resets []error
	gen    uint64
}

func (a *recAcker) Advance(gen uint64, replayID []byte, n int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if gen != a.gen {
		return
	}
	seq := binary.BigEndian.Uint64(replayID)
	if seq <= a.last && a.acked > 0 {
		a.t.Errorf("progress went backwards: %d after %d", seq, a.last)
	}
	a.last = seq
	a.acked += n
}

func (a *recAcker) Reset(gen uint64, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if gen == a.gen {
		a.resets = append(a.resets, err)
	}
}

func (a *recAcker) snapshot() (last uint64, acked, resets int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.last, a.acked, len(a.resets)
}

func replay(seq uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, seq)
	return b
}

func TestAckTrackerOrderingUnderConcurrency(t *testing.T) {
	for iter := 0; iter < 50; iter++ {
		tr := newAckTracker("t")
		acker := &recAcker{t: t}
		ref := &sink.SubscriptionRef{Acker: acker}
		const batches = 200
		var wg sync.WaitGroup
		appended := make(chan int64, batches)
		wg.Add(2)
		go func() { // writer
			defer wg.Done()
			for i := int64(0); i < batches; i++ {
				tr.append(pending{offset: i, n: 1, updates: []update{{ref: ref, replayID: replay(uint64(i + 1)), n: 1}}})
				appended <- i
			}
			close(appended)
		}()
		go func() { // dispatcher: acks may run ahead of appends
			defer wg.Done()
			var off int64 = -1
			for off < batches-1 {
				off += int64(rand.IntN(5))
				if off > batches-1 {
					off = batches - 1
				}
				tr.ackThrough(off)
			}
		}()
		wg.Wait()
		if last, acked, _ := acker.snapshot(); acked != batches || last != batches {
			t.Fatalf("iter %d: acked=%d last=%d", iter, acked, last)
		}
		if tr.len() != 0 {
			t.Fatalf("tracker not drained: %d", tr.len())
		}
	}
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newTestSink(f *MemoryFactory, ensure func(context.Context, string) error) *Sink {
	return New(f, Options{
		SlotsForTable: func(int) int { return 2 },
		BatchLinger:   time.Millisecond,
		Reopen:        backoff.Policy{Base: 5 * time.Millisecond, Max: 20 * time.Millisecond},
		EnsureTable:   ensure,
		Logger:        quietLogger(),
	})
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func submitWithRetry(t *testing.T, w sink.Writer, rec *sink.Record) {
	t.Helper()
	for {
		err := w.Submit(context.Background(), rec)
		if err == nil {
			return
		}
		if !errors.Is(err, sink.ErrUnavailable) {
			t.Fatalf("submit: %v", err)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestSinkDeliversAndFlushesOnClose(t *testing.T) {
	f := NewMemoryFactory(0)
	var ensured atomic.Int32
	s := newTestSink(f, func(context.Context, string) error { ensured.Add(1); return nil })
	s.Prepare(context.Background(), map[string]int{"a.b.c": 10})
	waitFor(t, "ready", s.Ready)

	const subs, perSub = 10, 300
	ackers := make([]*recAcker, subs)
	var wg sync.WaitGroup
	for i := 0; i < subs; i++ {
		ackers[i] = &recAcker{t: t}
		w, err := s.Open(&sink.SubscriptionRef{Key: tenant.SubKey{Tenant: tenant.Key(fmt.Sprint("t", i)), Topic: "/data/X"}, Table: "a.b.c", Acker: ackers[i]})
		if err != nil {
			t.Fatal(err)
		}
		wg.Add(1)
		go func(w sink.Writer) {
			defer wg.Done()
			for j := 1; j <= perSub; j++ {
				submitWithRetry(t, w, &sink.Record{Payload: []byte("row"), ReplayID: replay(uint64(j))})
			}
		}(w)
	}
	wg.Wait()
	if err := s.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	for i, a := range ackers {
		if last, acked, _ := a.snapshot(); acked != perSub || last != perSub {
			t.Errorf("sub %d: acked=%d last=%d, want all %d after Close", i, acked, last, perSub)
		}
	}
	if got := len(f.Rows("a.b.c")); got != subs*perSub {
		t.Errorf("rows = %d", got)
	}
	if ensured.Load() != 1 {
		t.Errorf("EnsureTable called %d times", ensured.Load())
	}
	if _, err := s.Open(&sink.SubscriptionRef{Table: "a.b.c"}); !errors.Is(err, sink.ErrClosed) {
		t.Errorf("Open after Close = %v", err)
	}
}

func TestSinkStreamFailureResetsAndRecovers(t *testing.T) {
	f := NewMemoryFactory(20 * time.Millisecond) // acks lag, so records are in flight at failure
	s := newTestSink(f, nil)
	defer s.Close(context.Background())
	s.Prepare(context.Background(), map[string]int{"a.b.c": 1})
	waitFor(t, "ready", s.Ready)

	a := &recAcker{t: t}
	w, _ := s.Open(&sink.SubscriptionRef{Key: tenant.SubKey{Tenant: "acme", Topic: "/data/X"}, Table: "a.b.c", Acker: a})
	for j := 1; j <= 5; j++ {
		submitWithRetry(t, w, &sink.Record{Payload: []byte("x"), ReplayID: replay(uint64(j))})
	}
	time.Sleep(5 * time.Millisecond) // let the writer ingest the batch
	if n := f.FailStreams("a.b.c", errors.New("server closed stream")); n == 0 {
		t.Fatal("no streams failed")
	}
	waitFor(t, "reset", func() bool { _, _, r := a.snapshot(); return r > 0 })

	// The subscription restarts (new generation) and the slot reopens.
	a.mu.Lock()
	a.gen = 1
	a.mu.Unlock()
	waitFor(t, "reopen", func() bool { return f.Opened() >= 3 })
	for j := 6; j <= 8; j++ {
		submitWithRetry(t, w, &sink.Record{Payload: []byte("x"), ReplayID: replay(uint64(j)), Gen: 1})
	}
	waitFor(t, "new acks", func() bool { last, _, _ := a.snapshot(); return last == 8 })
	health := s.Health()
	var failures int
	for _, h := range health {
		failures += h.Failures
	}
	if failures == 0 {
		t.Errorf("health should record the failure: %+v", health)
	}
}

func TestSinkOpenFailureIsUnavailableThenRecovers(t *testing.T) {
	f := NewMemoryFactory(0)
	f.FailOpens("a.b.c", 3)
	s := newTestSink(f, nil)
	defer s.Close(context.Background())
	s.Prepare(context.Background(), map[string]int{"a.b.c": 1})
	a := &recAcker{t: t}
	w, _ := s.Open(&sink.SubscriptionRef{Key: tenant.SubKey{Tenant: "acme", Topic: "/data/X"}, Table: "a.b.c", Acker: a})
	sawUnavailable := false
	for {
		err := w.Submit(context.Background(), &sink.Record{Payload: []byte("x"), ReplayID: replay(1)})
		if err == nil {
			break
		}
		if !errors.Is(err, sink.ErrUnavailable) {
			t.Fatal(err)
		}
		sawUnavailable = true
		time.Sleep(time.Millisecond)
	}
	if !sawUnavailable {
		t.Error("expected ErrUnavailable while opens fail")
	}
	waitFor(t, "ack", func() bool { _, acked, _ := a.snapshot(); return acked == 1 })
}

func TestDescriptor(t *testing.T) {
	d, err := Descriptor()
	if err != nil || len(d) == 0 {
		t.Fatalf("Descriptor: %v", err)
	}
}
