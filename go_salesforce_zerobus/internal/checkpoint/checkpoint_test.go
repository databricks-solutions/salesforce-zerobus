package checkpoint

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
)

var k1 = Key{Tenant: "acme", Topic: "/data/AccountChangeEvent"}

func TestWatermarkGenerationsAndCredit(t *testing.T) {
	reg := NewRegistry()
	w := reg.Register(k1, "a.b.c", "", []byte{1})
	gen := w.Restart()

	if !w.BeginSubmit(gen) {
		t.Fatal("BeginSubmit current gen")
	}
	w.EndSubmit(gen, []byte{2}, true)
	w.BeginSubmit(gen)
	w.EndSubmit(gen, []byte{3}, true)
	if w.Unacked() != 2 || !bytes.Equal(w.ResumePoint(), []byte{3}) {
		t.Fatalf("unacked=%d resume=%v", w.Unacked(), w.ResumePoint())
	}

	w.Advance(gen, []byte{2}, 1)
	select {
	case <-w.Notify():
	default:
		t.Fatal("Advance should notify")
	}
	if w.Unacked() != 1 || !bytes.Equal(w.Acked(), []byte{2}) {
		t.Fatalf("after ack: unacked=%d acked=%v", w.Unacked(), w.Acked())
	}

	// Sink reset for the current generation is delivered; a stale one is not.
	w.Reset(gen-1, errors.New("stale"))
	w.Reset(gen, errors.New("slot died"))
	if err := <-w.ResetCh(); err.Error() != "slot died" {
		t.Fatalf("reset err = %v", err)
	}

	// Restart: in-flight record 3 is forgotten; resume from acked (2).
	gen2 := w.Restart()
	if w.Unacked() != 0 || !bytes.Equal(w.ResumePoint(), []byte{2}) {
		t.Fatalf("after restart: unacked=%d resume=%v", w.Unacked(), w.ResumePoint())
	}
	// A late ack from the old generation must not move the watermark.
	w.Advance(gen, []byte{3}, 1)
	if !bytes.Equal(w.Acked(), []byte{2}) {
		t.Fatalf("stale ack advanced watermark to %v", w.Acked())
	}
	if w.BeginSubmit(gen) {
		t.Fatal("BeginSubmit with stale gen should fail")
	}
	w.BeginSubmit(gen2)
	w.EndSubmit(gen2, nil, false)
	if w.Unacked() != 0 {
		t.Fatalf("failed submit should release credit: %d", w.Unacked())
	}
}

func TestAdvanceIdleOnlyWhenNothingInFlight(t *testing.T) {
	w := NewRegistry().Register(k1, "a.b.c", "00D", nil)
	gen := w.Restart()
	w.BeginSubmit(gen)
	if w.AdvanceIdle(gen, []byte{9}) {
		t.Fatal("must not advance past in-flight records")
	}
	w.EndSubmit(gen, []byte{5}, true)
	w.Advance(gen, []byte{5}, 1)
	if !w.AdvanceIdle(gen, []byte{9}) || !bytes.Equal(w.Acked(), []byte{9}) {
		t.Fatalf("idle advance failed: %v", w.Acked())
	}
}

func TestCommitter(t *testing.T) {
	ctx := context.Background()
	reg := NewRegistry()
	store := NewMemoryStore()
	c := &Committer{Registry: reg, Store: store, Owner: "pod-0", ChunkSize: 2, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}

	var ws []*Watermark
	for i, topic := range []string{"/data/A", "/data/B", "/data/C"} {
		w := reg.Register(Key{"acme", topic}, "a.b.c", "00D", nil)
		gen := w.Restart()
		w.Advance(gen, []byte{byte(i + 1)}, 3)
		ws = append(ws, w)
	}
	noOrg := reg.Register(Key{"globex", "/data/A"}, "a.b.c", "", nil) // org unknown: not saved yet
	noOrg.Advance(noOrg.Restart(), []byte{7}, 1)

	if err := c.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if store.Saves != 2 {
		t.Errorf("3 rows with chunk size 2 should take 2 saves, got %d", store.Saves)
	}
	cp, ok := store.Get(Key{"acme", "/data/B"})
	if !ok || !bytes.Equal(cp.ReplayID, []byte{2}) || cp.EventsAcked != 3 || cp.Owner != "pod-0" || cp.OrgID != "00D" {
		t.Fatalf("stored = %+v", cp)
	}
	if _, ok := store.Get(Key{"globex", "/data/A"}); ok {
		t.Error("checkpoint without org ID must not be saved")
	}

	// Nothing dirty: no save.
	saves := store.Saves
	c.Flush(ctx)
	if store.Saves != saves {
		t.Error("clean watermarks should not be saved")
	}

	// A failed save keeps progress dirty and the delta intact for the retry.
	gen := ws[0].Gen()
	ws[0].Advance(gen, []byte{10}, 2)
	store.Err = errors.New("lakebase down")
	if err := c.Flush(ctx); err == nil {
		t.Fatal("expected save error")
	}
	store.Err = nil
	ws[0].Advance(gen, []byte{11}, 1)
	if err := c.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	cp, _ = store.Get(Key{"acme", "/data/A"})
	if !bytes.Equal(cp.ReplayID, []byte{11}) || cp.EventsAcked != 6 {
		t.Fatalf("after retry: %+v (want replay 11, 3+2+1 acked)", cp)
	}
	if st := ws[0].Status(); st.Dirty || !bytes.Equal(st.Committed, []byte{11}) {
		t.Fatalf("status after commit: %+v", st)
	}

	// Released watermarks are dropped once committed.
	reg.Release(Key{"acme", "/data/C"})
	c.Flush(ctx)
	if _, ok := reg.Get(Key{"acme", "/data/C"}); ok {
		t.Error("released, committed watermark should be pruned")
	}
	// Re-registering keeps progress.
	w := reg.Register(Key{"acme", "/data/A"}, "x.y.z", "00D", []byte{0})
	if !bytes.Equal(w.Acked(), []byte{11}) {
		t.Errorf("re-register lost progress: %v", w.Acked())
	}
}
