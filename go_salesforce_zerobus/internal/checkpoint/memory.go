package checkpoint

import (
	"context"
	"sync"
)

// MemoryStore is an in-process Store for tests and load runs.
type MemoryStore struct {
	mu    sync.Mutex
	rows  map[Key]Checkpoint
	Saves int // number of SaveMany calls
	Err   error
}

// NewMemoryStore returns an empty store.
func NewMemoryStore() *MemoryStore { return &MemoryStore{rows: map[Key]Checkpoint{}} }

func (m *MemoryStore) Init(context.Context) error { return nil }

func (m *MemoryStore) LoadMany(_ context.Context, keys []Key) (map[Key]Checkpoint, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[Key]Checkpoint{}
	for _, k := range keys {
		if cp, ok := m.rows[k]; ok {
			out[k] = cp
		}
	}
	return out, nil
}

func (m *MemoryStore) SaveMany(_ context.Context, cps []Checkpoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Err != nil {
		return m.Err
	}
	m.Saves++
	for _, cp := range cps {
		prev := m.rows[cp.Key]
		cp.EventsAcked += prev.EventsAcked
		cp.ReplayID = clone(cp.ReplayID)
		m.rows[cp.Key] = cp
	}
	return nil
}

func (m *MemoryStore) Delete(_ context.Context, k Key) error {
	m.mu.Lock()
	delete(m.rows, k)
	m.mu.Unlock()
	return nil
}

func (m *MemoryStore) Close() error { return nil }

// Get returns the stored checkpoint for k.
func (m *MemoryStore) Get(k Key) (Checkpoint, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp, ok := m.rows[k]
	return cp, ok
}

// Put seeds a checkpoint.
func (m *MemoryStore) Put(cp Checkpoint) {
	m.mu.Lock()
	m.rows[cp.Key] = cp
	m.mu.Unlock()
}
