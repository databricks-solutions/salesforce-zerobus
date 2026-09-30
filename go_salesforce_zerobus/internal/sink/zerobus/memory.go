package zerobus

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	zb "github.com/databricks/zerobus-sdk/purego/zerobus"
)

// MemoryFactory is an in-process stand-in for Zerobus used by tests and load
// runs: records are kept in memory and acked asynchronously, in order, after
// AckLatency. It supports fault injection.
type MemoryFactory struct {
	AckLatency time.Duration
	// KeepRows retains ingested payloads (disable for long load runs).
	KeepRows bool

	mu        sync.Mutex
	rows      map[string][][]byte
	streams   map[*memStream]struct{}
	openFails map[string]int
	ingested  atomic.Int64
	opened    atomic.Int64
}

// NewMemoryFactory returns a factory that keeps rows.
func NewMemoryFactory(ackLatency time.Duration) *MemoryFactory {
	return &MemoryFactory{AckLatency: ackLatency, KeepRows: true, rows: map[string][][]byte{},
		streams: map[*memStream]struct{}{}, openFails: map[string]int{}}
}

// Open opens a stream (or fails if FailOpens was armed for table).
func (f *MemoryFactory) Open(ctx context.Context, table string, cb zb.AckCallback) (Stream, error) {
	f.mu.Lock()
	if f.openFails[table] > 0 {
		f.openFails[table]--
		f.mu.Unlock()
		return nil, fmt.Errorf("memory: open %s: injected failure", table)
	}
	s := &memStream{f: f, table: table, cb: cb, acks: make(chan int64, 1<<16), done: make(chan struct{}),
		id: fmt.Sprintf("mem-%d", f.opened.Add(1))}
	f.streams[s] = struct{}{}
	f.mu.Unlock()
	go s.ackLoop()
	return s, nil
}

// Close implements Factory.
func (f *MemoryFactory) Close() error { return nil }

// FailOpens makes the next n opens of table fail.
func (f *MemoryFactory) FailOpens(table string, n int) {
	f.mu.Lock()
	f.openFails[table] += n
	f.mu.Unlock()
}

// FailStreams terminally fails every open stream of table, as a server-side
// failure would: unacked records are reported through OnError.
func (f *MemoryFactory) FailStreams(table string, err error) int {
	f.mu.Lock()
	var victims []*memStream
	for s := range f.streams {
		if s.table == table {
			victims = append(victims, s)
		}
	}
	f.mu.Unlock()
	for _, s := range victims {
		s.terminate(err)
	}
	return len(victims)
}

// Rows returns the payloads ingested into table.
func (f *MemoryFactory) Rows(table string) [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]byte(nil), f.rows[table]...)
}

// Ingested returns the total number of records ingested.
func (f *MemoryFactory) Ingested() int64 { return f.ingested.Load() }

// Opened returns how many streams were opened.
func (f *MemoryFactory) Opened() int64 { return f.opened.Load() }

var errMemClosed = errors.New("memory: stream closed")

type memStream struct {
	f     *MemoryFactory
	table string
	cb    zb.AckCallback
	id    string
	acks  chan int64
	done  chan struct{}

	mu       sync.Mutex
	next     int64
	acked    int64
	closed   bool
	err      error
	stopOnce sync.Once
}

func (s *memStream) ID() string { return s.id }

func (s *memStream) IngestRecordsOffsetContext(ctx context.Context, records [][]byte) (int64, error) {
	if err := ctx.Err(); err != nil {
		return -1, err
	}
	s.mu.Lock()
	if s.closed {
		err := s.err
		s.mu.Unlock()
		if err == nil {
			err = errMemClosed
		}
		return -1, err
	}
	off := s.next
	s.next++
	s.mu.Unlock()
	if s.f.KeepRows {
		s.f.mu.Lock()
		for _, r := range records {
			s.f.rows[s.table] = append(s.f.rows[s.table], append([]byte(nil), r...))
		}
		s.f.mu.Unlock()
	}
	s.f.ingested.Add(int64(len(records)))
	select {
	case s.acks <- off:
	case <-s.done:
	}
	return off, nil
}

func (s *memStream) ackLoop() {
	for {
		select {
		case <-s.done:
			return
		case off := <-s.acks:
			if s.f.AckLatency > 0 {
				select {
				case <-time.After(s.f.AckLatency):
				case <-s.done:
					return
				}
			}
			s.mu.Lock()
			if s.closed && s.err != nil {
				s.mu.Unlock()
				return
			}
			s.acked = off + 1
			s.mu.Unlock()
			s.cb.OnAck(off)
		}
	}
}

// Close flushes: waits until every ingested record is acked.
func (s *memStream) Close() error {
	s.mu.Lock()
	if s.err != nil {
		err := s.err
		s.mu.Unlock()
		s.stop()
		return err
	}
	s.closed = true
	s.mu.Unlock()
	deadline := time.Now().Add(10 * time.Second)
	for {
		s.mu.Lock()
		done := s.acked >= s.next
		s.mu.Unlock()
		if done {
			break
		}
		if time.Now().After(deadline) {
			s.stop()
			return errors.New("memory: flush timeout")
		}
		time.Sleep(time.Millisecond)
	}
	s.stop()
	return nil
}

func (s *memStream) IsClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *memStream) terminate(err error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed, s.err = true, err
	first := s.acked
	s.mu.Unlock()
	s.stop()
	s.cb.OnError(first, err)
}

func (s *memStream) stop() {
	s.stopOnce.Do(func() {
		close(s.done)
		s.f.mu.Lock()
		delete(s.f.streams, s)
		s.f.mu.Unlock()
	})
}
