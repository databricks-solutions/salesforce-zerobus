package checkpoint

import "context"

// WithFallback returns a Store that reads from primary and, for keys it does
// not have, from fallback (e.g. seed Lakebase from the Delta table on first
// run). Writes go to primary only.
func WithFallback(primary, fallback Store) Store {
	return &fallbackStore{Store: primary, fallback: fallback}
}

type fallbackStore struct {
	Store
	fallback Store
}

func (f *fallbackStore) LoadMany(ctx context.Context, keys []Key) (map[Key]Checkpoint, error) {
	got, err := f.Store.LoadMany(ctx, keys)
	if err != nil {
		return nil, err
	}
	var missing []Key
	for _, k := range keys {
		if _, ok := got[k]; !ok {
			missing = append(missing, k)
		}
	}
	if len(missing) == 0 {
		return got, nil
	}
	seed, err := f.fallback.LoadMany(ctx, missing)
	if err != nil {
		return nil, err
	}
	for k, cp := range seed {
		got[k] = cp
	}
	return got, nil
}

func (f *fallbackStore) Close() error {
	f.fallback.Close()
	return f.Store.Close()
}
