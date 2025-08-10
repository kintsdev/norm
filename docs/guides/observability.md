## Observability & Logging

Logging and metrics are pluggable. Options:

```go
db, _ := norm.New(cfg,
    norm.WithLogger(norm.StdLogger{}),
    norm.WithLogMode(norm.LogInfo),
    norm.WithMetrics(norm.ExpvarMetrics{}),
    norm.WithLogContextFields(func(ctx context.Context) []norm.Field {
        if v := ctx.Value("corr_id"); v != nil { return []norm.Field{{Key: "corr_id", Value: v}} }
        return nil
    }),
    norm.WithSlowQueryThreshold(100*time.Millisecond),
    norm.WithLogParameterMasking(true),
)
```

Expose `expvar` metrics at `/debug/vars` using the example in `examples/observability/main.go`.


