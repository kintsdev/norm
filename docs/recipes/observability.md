## Observability

Enable structured logging, metrics, slow query logging, and parameter masking.

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

See `examples/observability/main.go` for an HTTP `/debug/vars` demo.


