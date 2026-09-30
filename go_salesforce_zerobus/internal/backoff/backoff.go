// Package backoff provides jittered exponential backoff helpers.
package backoff

import (
	"context"
	"math"
	"math/rand/v2"
	"time"
)

// Policy describes an exponential backoff schedule.
type Policy struct {
	Base time.Duration
	Max  time.Duration
}

// Delay returns the delay for attempt (0-based) under p.
func (p Policy) Delay(attempt int) time.Duration {
	return Calculate(attempt, p.Base, p.Max)
}

// Calculate returns the backoff delay for a given attempt.
// Formula: min(base * 2^attempt, max) + jitter(10-20%).
func Calculate(attempt int, base, max time.Duration) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	delay := float64(base) * math.Pow(2, float64(attempt))
	if delay > float64(max) || math.IsInf(delay, 0) {
		delay = float64(max)
	}
	jitter := delay * (0.1 + rand.Float64()*0.1)
	return time.Duration(delay + jitter)
}

// Sleep performs a context-cancellable backoff sleep.
// Returns ctx.Err() if the context is cancelled during the sleep.
func Sleep(ctx context.Context, attempt int, base, max time.Duration) error {
	return SleepFor(ctx, Calculate(attempt, base, max))
}

// SleepFor sleeps for d or until ctx is done.
func SleepFor(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
