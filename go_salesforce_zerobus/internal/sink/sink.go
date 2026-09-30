// Package sink defines where subscriptions deliver rows.
package sink

import (
	"context"
	"errors"
	"time"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/tenant"
)

var (
	// ErrUnavailable means the stream serving the subscription is down;
	// retry after backing off.
	ErrUnavailable = errors.New("sink: stream unavailable")
	// ErrClosed means the sink is shutting down.
	ErrClosed = errors.New("sink: closed")
)

// Record is one encoded row.
type Record struct {
	Payload    []byte
	ReplayID   []byte
	Gen        uint64 // subscription generation that produced the record
	ReceivedAt time.Time
}

// Acker receives durability notifications for a subscription. Both methods
// must return promptly and must not call back into the sink.
type Acker interface {
	// Advance reports that n records of generation gen, the last with
	// replayID, are durable. Calls for one subscription arrive in order.
	Advance(gen uint64, replayID []byte, n int)
	// Reset reports that in-flight records of generation gen were lost; the
	// subscription must resume from its last acked position.
	Reset(gen uint64, err error)
}

// SubscriptionRef identifies a subscription to the sink.
type SubscriptionRef struct {
	Key   tenant.SubKey
	Table string
	Acker Acker
}

// Writer submits rows for one subscription. Submit blocks for
// backpressure and is not safe for concurrent use.
type Writer interface {
	Submit(ctx context.Context, r *Record) error
	Close()
}

// StreamHealth describes one stream slot.
type StreamHealth struct {
	Table         string    `json:"table"`
	Slot          int       `json:"slot"`
	State         string    `json:"state"`
	StreamID      string    `json:"stream_id,omitempty"`
	Subscriptions int       `json:"subscriptions"`
	Failures      int       `json:"failures"`
	LastError     string    `json:"last_error,omitempty"`
	LastErrorAt   time.Time `json:"last_error_at,omitempty"`
}

// Sink delivers rows to target tables.
type Sink interface {
	// Prepare ensures pools exist for tables (value: expected subscription
	// count, used to size new pools). Existing pools are kept.
	Prepare(ctx context.Context, tables map[string]int) error
	// Open returns a writer pinned to one stream slot of ref.Table.
	Open(ref *SubscriptionRef) (Writer, error)
	// Close drains queued rows, flushes streams, applies final acks, and
	// releases connections.
	Close(ctx context.Context) error
	Health() []StreamHealth
	// Ready reports whether every stream slot has opened at least once.
	Ready() bool
}
