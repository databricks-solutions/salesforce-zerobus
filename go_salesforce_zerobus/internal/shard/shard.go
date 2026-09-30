// Package shard assigns tenants to service replicas.
package shard

import (
	"context"
	"fmt"
	"hash/fnv"
	"os"
	"regexp"
	"strconv"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/tenant"
)

// Assigner decides which tenants this replica owns.
type Assigner interface {
	// Filter returns the subset of s owned by this replica.
	Filter(ctx context.Context, s tenant.Snapshot) (tenant.Snapshot, error)
	// Changes signals that ownership may have changed (e.g. a lease moved).
	// Static assigners return nil.
	Changes() <-chan struct{}
}

// StaticHash owns tenants whose jump hash of the tenant key equals Index, or
// whose shard pin equals Index. Only ~1/Count of tenants move when Count
// grows by one.
type StaticHash struct {
	Index, Count int
}

// NewStaticHash validates and returns a static assigner.
func NewStaticHash(index, count int) (*StaticHash, error) {
	if count < 1 || index < 0 || index >= count {
		return nil, fmt.Errorf("invalid shard %d of %d", index, count)
	}
	return &StaticHash{Index: index, Count: count}, nil
}

// Owner returns the shard that owns t.
func (s *StaticHash) Owner(t tenant.Tenant) int {
	if t.ShardPin != nil {
		return *t.ShardPin
	}
	return int(Jump(KeyHash(string(t.Key)), int32(s.Count)))
}

func (s *StaticHash) Filter(_ context.Context, snap tenant.Snapshot) (tenant.Snapshot, error) {
	out := tenant.Snapshot{Version: snap.Version}
	for _, t := range snap.Tenants {
		if s.Owner(t) == s.Index {
			out.Tenants = append(out.Tenants, t)
		}
	}
	return out, nil
}

func (s *StaticHash) Changes() <-chan struct{} { return nil }

// KeyHash hashes a tenant key (FNV-1a 64).
func KeyHash(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}

// Jump is Lamping & Veach's jump consistent hash.
func Jump(key uint64, buckets int32) int32 {
	var b, j int64 = -1, 0
	for j < int64(buckets) {
		b = j
		key = key*2862933555777941757 + 1
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}
	return int32(b)
}

var ordinalPattern = regexp.MustCompile(`-(\d+)$`)

// ResolveIndex parses a shard index. "auto" takes the trailing ordinal of
// the hostname (StatefulSet pods are named <name>-<ordinal>).
func ResolveIndex(v string) (int, error) {
	if v != "auto" {
		return strconv.Atoi(v)
	}
	host, err := os.Hostname()
	if err != nil {
		return 0, err
	}
	m := ordinalPattern.FindStringSubmatch(host)
	if m == nil {
		return 0, fmt.Errorf("shard index auto: hostname %q has no -<ordinal> suffix", host)
	}
	return strconv.Atoi(m[1])
}
