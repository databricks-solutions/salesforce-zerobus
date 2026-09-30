package checkpoint

import (
	"sort"
	"sync"
)

// Registry holds the watermarks of owned subscriptions.
type Registry struct {
	mu sync.Mutex
	m  map[Key]*Watermark
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{m: map[Key]*Watermark{}} }

// Register returns the watermark for k, creating it at initial if new. An
// existing watermark (e.g. a subscription restarted after a config change)
// keeps its progress; only the target table is updated.
func (r *Registry) Register(k Key, table, orgID string, initial []byte) *Watermark {
	r.mu.Lock()
	defer r.mu.Unlock()
	if w, ok := r.m[k]; ok {
		w.setTable(table)
		return w
	}
	w := newWatermark(k, table, orgID, initial)
	r.m[k] = w
	return w
}

// Get returns the watermark for k.
func (r *Registry) Get(k Key) (*Watermark, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	w, ok := r.m[k]
	return w, ok
}

// Release marks k as no longer owned; it is dropped once its progress is
// committed.
func (r *Registry) Release(k Key) {
	r.mu.Lock()
	w, ok := r.m[k]
	r.mu.Unlock()
	if ok {
		w.release()
	}
}

// All returns every watermark, sorted by key.
func (r *Registry) All() []*Watermark {
	r.mu.Lock()
	out := make([]*Watermark, 0, len(r.m))
	for _, w := range r.m {
		out = append(out, w)
	}
	r.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].key.String() < out[j].key.String() })
	return out
}

func (r *Registry) prune() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for k, w := range r.m {
		if w.removable() {
			delete(r.m, k)
		}
	}
}
