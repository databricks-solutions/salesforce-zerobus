// Package checkpoint tracks per-subscription replay progress and persists it.
//
// Invariant: a stored replay ID is never ahead of the last replay ID Zerobus
// durably acknowledged for that subscription. Progress only advances inside
// the sink's ack path (Watermark.Advance), acks within a pinned stream are
// ordered, and acks from an earlier generation of a restarted subscription
// are discarded. Delivery is therefore at-least-once.
package checkpoint

import (
	"context"
	"time"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/tenant"
)

// Key identifies a checkpoint row.
type Key struct {
	Tenant string
	Topic  string
}

// KeyOf converts a subscription key.
func KeyOf(k tenant.SubKey) Key { return Key{Tenant: string(k.Tenant), Topic: k.Topic} }

func (k Key) String() string { return k.Tenant + "|" + k.Topic }

// Checkpoint is a persisted resume position.
type Checkpoint struct {
	Key         Key
	OrgID       string
	Table       string
	ReplayID    []byte
	EventsAcked int64 // delta when saving; running total when loaded
	Owner       string
	UpdatedAt   time.Time
}

// Store persists checkpoints.
type Store interface {
	// Init prepares the store (e.g. creates tables). Idempotent.
	Init(ctx context.Context) error
	// LoadMany returns stored checkpoints for keys; missing keys are absent.
	LoadMany(ctx context.Context, keys []Key) (map[Key]Checkpoint, error)
	// SaveMany upserts checkpoints. EventsAcked is added to the stored total.
	SaveMany(ctx context.Context, cps []Checkpoint) error
	// Delete removes a checkpoint (operator reset).
	Delete(ctx context.Context, k Key) error
	Close() error
}
