package pubsub

import (
	"container/list"
	"context"
	"fmt"
	"sync"

	"github.com/linkedin/goavro/v2"
	"golang.org/x/sync/singleflight"

	"github.com/databricks-solutions/salesforce-zerobus/go_salesforce_zerobus/internal/obs"
)

// Schema is a compiled Salesforce Avro schema.
type Schema struct {
	ID     string
	JSON   string
	Codec  *goavro.Codec // standard-JSON codec: decodes binary, emits unwrapped JSON
	Bitmap *BitmapSchema
	size   int64
}

// CompileSchema compiles an Avro schema for decoding and bitmap resolution.
func CompileSchema(id, schemaJSON string) (*Schema, error) {
	codec, err := goavro.NewCodecForStandardJSONFull(schemaJSON)
	if err != nil {
		return nil, fmt.Errorf("compiling Avro schema %s: %w", id, err)
	}
	bm, err := ParseBitmapSchema(schemaJSON)
	if err != nil {
		return nil, err
	}
	// Rough retained size: raw JSON dominates; the compiled codec is a small
	// multiple of it.
	return &Schema{ID: id, JSON: schemaJSON, Codec: codec, Bitmap: bm, size: int64(len(schemaJSON)) * 4}, nil
}

// SchemaFetcher fetches the JSON for schemaID (the GetSchema RPC).
type SchemaFetcher func(ctx context.Context, schemaID string) (string, error)

// SchemaCache is a byte-bounded LRU of compiled schemas shared by every
// subscription on the replica. Entries are keyed by (org, schema ID): CDC
// schemas include org-specific custom fields, so IDs are not assumed to be
// globally unique. Concurrent misses for the same key share one fetch.
type SchemaCache struct {
	maxBytes int64

	mu    sync.Mutex
	bytes int64
	ll    *list.List
	items map[string]*list.Element

	group singleflight.Group
}

type schemaEntry struct {
	key    string
	schema *Schema
}

// NewSchemaCache creates a cache holding roughly maxBytes of schemas.
func NewSchemaCache(maxBytes int64) *SchemaCache {
	if maxBytes <= 0 {
		maxBytes = 256 << 20
	}
	return &SchemaCache{maxBytes: maxBytes, ll: list.New(), items: make(map[string]*list.Element)}
}

// Get returns the schema for (orgID, schemaID), fetching and compiling it on
// a miss.
func (c *SchemaCache) Get(ctx context.Context, orgID, schemaID string, fetch SchemaFetcher) (*Schema, error) {
	key := orgID + "/" + schemaID
	if s := c.lookup(key); s != nil {
		obs.SchemaCacheHits.Inc()
		return s, nil
	}
	v, err, _ := c.group.Do(key, func() (any, error) {
		if s := c.lookup(key); s != nil {
			return s, nil
		}
		obs.SchemaCacheMisses.Inc()
		schemaJSON, err := fetch(ctx, schemaID)
		if err != nil {
			return nil, err
		}
		s, err := CompileSchema(schemaID, schemaJSON)
		if err != nil {
			return nil, err
		}
		c.add(key, s)
		return s, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*Schema), nil
}

func (c *SchemaCache) lookup(key string) *Schema {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		c.ll.MoveToFront(el)
		return el.Value.(*schemaEntry).schema
	}
	return nil
}

func (c *SchemaCache) add(key string, s *Schema) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		c.ll.MoveToFront(el)
		return
	}
	c.items[key] = c.ll.PushFront(&schemaEntry{key: key, schema: s})
	c.bytes += s.size
	// Always keep the newest entry, even if it alone exceeds the budget.
	for c.bytes > c.maxBytes && c.ll.Len() > 1 {
		el := c.ll.Back()
		e := el.Value.(*schemaEntry)
		c.ll.Remove(el)
		delete(c.items, e.key)
		c.bytes -= e.schema.size
	}
	obs.SchemaCacheBytes.Set(float64(c.bytes))
}

// Len returns the number of cached schemas.
func (c *SchemaCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ll.Len()
}
