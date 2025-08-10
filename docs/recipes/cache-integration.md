## Cache Integration

Provide a `Cache` implementation and use query hooks.

```go
db, _ := norm.New(cfg, norm.WithCache(norm.NoopCache{}))

var rows []map[string]any
// read-through
_ = db.Query().Table("users").WithCacheKey("users:first", 10*time.Second).Limit(1).Find(ctx, &rows)
// write-through invalidation
_ = db.Query().Raw("UPDATE users SET updated_at = NOW() WHERE id = $1", 1).WithInvalidateKeys("users:first").Exec(ctx)
```


