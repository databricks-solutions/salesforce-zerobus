package pubsub

import (
	lru "github.com/hashicorp/golang-lru/v2"
)

// DedupCache provides LRU-based event deduplication.
type DedupCache struct {
	cache *lru.Cache[string, struct{}]
}

// NewDedupCache creates a dedup cache with the given maximum size.
func NewDedupCache(size int) (*DedupCache, error) {
	c, err := lru.New[string, struct{}](size)
	if err != nil {
		return nil, err
	}
	return &DedupCache{cache: c}, nil
}

// IsDuplicate returns true if the event ID has been seen before.
func (d *DedupCache) IsDuplicate(eventID string) bool {
	return d.cache.Contains(eventID)
}

// MarkProcessed records an event ID as processed.
func (d *DedupCache) MarkProcessed(eventID string) {
	d.cache.Add(eventID, struct{}{})
}
