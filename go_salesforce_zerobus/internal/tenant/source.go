package tenant

import "context"

// Source produces tenant snapshots. The first snapshot is delivered before
// Watch returns successfully; each later snapshot replaces the previous one
// entirely (the supervisor reconciles the difference).
type Source interface {
	Watch(ctx context.Context) (<-chan Snapshot, error)
}

// StaticSource always serves one snapshot (single-tenant env mode, and
// dev/test runs from a local tenants file).
type StaticSource struct{ Snapshot Snapshot }

func (s StaticSource) Watch(context.Context) (<-chan Snapshot, error) {
	ch := make(chan Snapshot, 1)
	ch <- s.Snapshot
	return ch, nil
}
