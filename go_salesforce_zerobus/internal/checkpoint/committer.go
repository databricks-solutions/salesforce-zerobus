package checkpoint

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/obs"
)

// Committer periodically persists dirty watermarks in batches (one upsert
// per chunk), so thousands of subscriptions cost one round trip per interval.
type Committer struct {
	Registry  *Registry
	Store     Store
	Interval  time.Duration
	Owner     string // e.g. pod name, stored for diagnostics
	ChunkSize int
	Logger    *slog.Logger

	mu sync.Mutex // serializes commits (Run vs Flush)
}

// Run commits every Interval until ctx is done. Commit failures are logged
// and retried on the next tick; ingestion continues regardless (only the
// resume position lags).
func (c *Committer) Run(ctx context.Context) error {
	interval := c.Interval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			if err := c.Flush(ctx); err != nil && ctx.Err() == nil {
				c.Logger.Warn("Checkpoint commit failed; will retry", "error", err)
			}
		}
	}
}

// Flush commits all dirty watermarks now.
func (c *Committer) Flush(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	type pending struct {
		w   *Watermark
		cp  Checkpoint
		seq uint64
	}
	var batch []pending
	var oldest time.Time
	now := time.Now()
	for _, w := range c.Registry.All() {
		cp, seq, since, ok := w.snapshot()
		if !ok {
			continue
		}
		cp.Owner = c.Owner
		cp.UpdatedAt = now
		batch = append(batch, pending{w, cp, seq})
		if oldest.IsZero() || since.Before(oldest) {
			oldest = since
		}
	}
	obs.CheckpointDirty.Set(float64(len(batch)))
	if oldest.IsZero() {
		obs.CheckpointOldestSeconds.Set(0)
	} else {
		obs.CheckpointOldestSeconds.Set(now.Sub(oldest).Seconds())
	}
	if len(batch) == 0 {
		c.Registry.prune()
		return nil
	}

	chunk := c.ChunkSize
	if chunk <= 0 {
		chunk = 1000
	}
	for start := 0; start < len(batch); start += chunk {
		end := min(start+chunk, len(batch))
		cps := make([]Checkpoint, 0, end-start)
		for _, p := range batch[start:end] {
			cps = append(cps, p.cp)
		}
		began := time.Now()
		if err := c.Store.SaveMany(ctx, cps); err != nil {
			obs.CheckpointErrors.Inc()
			return err
		}
		obs.CheckpointCommitSeconds.Observe(time.Since(began).Seconds())
		obs.CheckpointRows.Add(float64(len(cps)))
		for _, p := range batch[start:end] {
			p.w.markCommitted(p.seq, p.cp)
		}
	}
	c.Registry.prune()
	return nil
}
