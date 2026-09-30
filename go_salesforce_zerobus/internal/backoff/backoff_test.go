package backoff

import (
	"context"
	"testing"
	"time"
)

func TestCalculateBounds(t *testing.T) {
	for attempt := 0; attempt < 70; attempt++ {
		d := Calculate(attempt, time.Second, time.Minute)
		want := float64(time.Second) * float64(uint64(1)<<min(attempt, 62))
		if want > float64(time.Minute) {
			want = float64(time.Minute)
		}
		if float64(d) < want*1.1-1 || float64(d) > want*1.2+1 {
			t.Fatalf("attempt %d: delay %v outside [%v, %v]", attempt, d, time.Duration(want*1.1), time.Duration(want*1.2))
		}
	}
}

func TestSleepForCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := SleepFor(ctx, time.Hour); err == nil {
		t.Fatal("expected context error")
	}
}
