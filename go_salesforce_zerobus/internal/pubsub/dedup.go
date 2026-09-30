package pubsub

import (
	lru "github.com/hashicorp/golang-lru/v2"
)

// DedupCache is a small LRU of recently delivered event IDs, used to drop
// events Salesforce redelivers after a reconnect.
type DedupCache struct {
	cache *lru.Cache[string, struct{}]
}

// NewDedupCache creates a dedup cache holding up to size event IDs.
func NewDedupCache(size int) *DedupCache {
	if size <= 0 {
		size = 1
	}
	c, _ := lru.New[string, struct{}](size) // only errors on size <= 0
	return &DedupCache{cache: c}
}

// Seen reports whether eventID was marked delivered.
func (d *DedupCache) Seen(eventID string) bool {
	return eventID != "" && d.cache.Contains(eventID)
}

// Mark records eventID as delivered. Call only after the sink accepted it.
func (d *DedupCache) Mark(eventID string) {
	if eventID != "" {
		d.cache.Add(eventID, struct{}{})
	}
}
