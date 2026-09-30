package shard

import (
	"context"
	"fmt"
	"testing"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/tenant"
)

func TestJumpDistributionAndStability(t *testing.T) {
	const n = 20000
	counts := make([]int, 8)
	moved := 0
	for i := 0; i < n; i++ {
		h := KeyHash(fmt.Sprintf("tenant-%d", i))
		b8, b9 := Jump(h, 8), Jump(h, 9)
		counts[b8]++
		if b8 != b9 {
			moved++
			if b9 != 8 {
				t.Fatalf("key moved between existing buckets: %d -> %d", b8, b9)
			}
		}
	}
	for b, c := range counts {
		if c < n/8*85/100 || c > n/8*115/100 {
			t.Errorf("bucket %d has %d keys, expected ~%d", b, c, n/8)
		}
	}
	if frac := float64(moved) / n; frac < 0.08 || frac > 0.14 {
		t.Errorf("moved fraction %.3f, expected ~1/9", frac)
	}
}

func TestStaticHashFilter(t *testing.T) {
	pin := 1
	snap := tenant.Snapshot{Tenants: []tenant.Tenant{{Key: "a"}, {Key: "b"}, {Key: "c"}, {Key: "pinned", ShardPin: &pin}}}
	total := 0
	for i := 0; i < 3; i++ {
		a, err := NewStaticHash(i, 3)
		if err != nil {
			t.Fatal(err)
		}
		got, _ := a.Filter(context.Background(), snap)
		total += len(got.Tenants)
		for _, tn := range got.Tenants {
			if tn.Key == "pinned" && i != 1 {
				t.Errorf("pinned tenant owned by shard %d", i)
			}
		}
	}
	if total != len(snap.Tenants) {
		t.Errorf("each tenant must be owned exactly once; total %d", total)
	}
	if _, err := NewStaticHash(3, 3); err == nil {
		t.Error("expected error for index >= count")
	}
}

func TestResolveIndex(t *testing.T) {
	if i, err := ResolveIndex("2"); err != nil || i != 2 {
		t.Fatalf("ResolveIndex(2) = %d, %v", i, err)
	}
}
