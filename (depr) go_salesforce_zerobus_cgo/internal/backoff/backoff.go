package backoff

import (
	"context"
	"math"
	"math/rand/v2"
	"time"
)

// Calculate returns the backoff delay for a given attempt.
// Formula: min(base * 2^attempt, max) + jitter(10-20%).
func Calculate(attempt int, base, max time.Duration) time.Duration {
	delay := float64(base) * math.Pow(2, float64(attempt))
	if delay > float64(max) {
		delay = float64(max)
	}
	jitter := delay * (0.1 + rand.Float64()*0.1)
	return time.Duration(delay + jitter)
}

// Sleep performs a context-cancellable backoff sleep.
// Returns ctx.Err() if the context is cancelled during the sleep.
func Sleep(ctx context.Context, attempt int, base, max time.Duration) error {
	delay := Calculate(attempt, base, max)
	t := time.NewTimer(delay)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
