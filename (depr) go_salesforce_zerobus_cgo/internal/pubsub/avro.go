package pubsub

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/linkedin/goavro/v2"
	"google.golang.org/grpc/metadata"

	pb "github.com/databricks-solutions/go-salesforce-zerobus/proto/gen"
)

// SchemaCache provides thread-safe caching of compiled Avro codecs.
type SchemaCache struct {
	mu     sync.RWMutex
	cache  map[string]*goavro.Codec
	raw    map[string]string // schema_id -> raw JSON string
	logger *slog.Logger
}

// NewSchemaCache creates a new SchemaCache.
func NewSchemaCache(logger *slog.Logger) *SchemaCache {
	return &SchemaCache{
		cache:  make(map[string]*goavro.Codec),
		raw:    make(map[string]string),
		logger: logger,
	}
}

// GetOrFetch retrieves a cached Avro codec. On cache miss, fetches the schema
// from Salesforce via the GetSchema RPC and compiles it.
func (sc *SchemaCache) GetOrFetch(ctx context.Context, schemaID string, stub pb.PubSubClient, md metadata.MD) (*goavro.Codec, string, error) {
	sc.mu.RLock()
	if codec, ok := sc.cache[schemaID]; ok {
		rawJSON := sc.raw[schemaID]
		sc.mu.RUnlock()
		return codec, rawJSON, nil
	}
	sc.mu.RUnlock()

	// Fetch schema from Salesforce
	outCtx := metadata.NewOutgoingContext(ctx, md)
	resp, err := stub.GetSchema(outCtx, &pb.SchemaRequest{SchemaId: schemaID})
	if err != nil {
		return nil, "", fmt.Errorf("GetSchema RPC failed for %s: %w", schemaID, err)
	}

	schemaJSON := resp.GetSchemaJson()
	codec, err := goavro.NewCodec(schemaJSON)
	if err != nil {
		return nil, "", fmt.Errorf("compiling Avro schema %s: %w", schemaID, err)
	}

	sc.mu.Lock()
	sc.cache[schemaID] = codec
	sc.raw[schemaID] = schemaJSON
	sc.mu.Unlock()

	sc.logger.Debug("Cached new Avro schema", "schema_id", schemaID)
	return codec, schemaJSON, nil
}

// DecodeAvro decodes an Avro binary payload using the given codec.
func DecodeAvro(codec *goavro.Codec, payload []byte) (map[string]interface{}, error) {
	native, _, err := codec.NativeFromBinary(payload)
	if err != nil {
		return nil, fmt.Errorf("Avro decode failed: %w", err)
	}

	record, ok := native.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("Avro decoded value is not a map: %T", native)
	}
	return record, nil
}

