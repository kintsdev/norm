## Options

Enhance logging, metrics, cache, and log behavior via options.

```go
db, _ := norm.New(cfg,
  norm.WithLogger(norm.StdLogger{}),
  norm.WithLogMode(norm.LogInfo),
  norm.WithMetrics(norm.ExpvarMetrics{}),
  norm.WithCache(norm.NoopCache{}),
  norm.WithLogContextFields(func(ctx context.Context) []norm.Field { return nil }),
  norm.WithSlowQueryThreshold(150*time.Millisecond),
  norm.WithLogParameterMasking(true),
)
```

`LogMode` values: `LogSilent`, `LogError`, `LogWarn`, `LogInfo`, `LogDebug`.


