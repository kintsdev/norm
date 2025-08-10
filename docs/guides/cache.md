## Cache Interface

Plug in your own cache by implementing `Cache`.

```go
type Cache interface {
  Get(ctx context.Context, key string) ([]byte, bool, error)
  Set(ctx context.Context, key string, value []byte, ttl time.Duration) error
  Invalidate(ctx context.Context, keys ...string) error
}
```

Usage in builder:

```go
// Read-through
_ = db.Query().Table("users").WithCacheKey("users:first", 10*time.Second).Limit(1).Find(ctx, &rows)
// Invalidate after writes
_, _ = db.Query().Table("users").Set("username = ?", "u2").Where("id = ?", 1).WithInvalidateKeys("users:first").ExecUpdate(ctx, nil)
```

Note: caching currently targets `[]map[string]any` in the built-in hook.


