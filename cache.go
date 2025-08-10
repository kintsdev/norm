package norm

import (
	"context"
	"time"
)

// Cache provides optional read-through/write-through hooks
type Cache interface {
	Get(ctx context.Context, key string) ([]byte, bool, error)
	Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
	Invalidate(ctx context.Context, keys ...string) error
}

// NoopCache is a default no-op cache implementation
type NoopCache struct{}

func (NoopCache) Get(ctx context.Context, key string) ([]byte, bool, error) { return nil, false, nil }
func (NoopCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return nil
}
func (NoopCache) Invalidate(ctx context.Context, keys ...string) error { return nil }
