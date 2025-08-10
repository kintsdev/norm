## Metrics

Provide your own collector by implementing `Metrics` or use `ExpvarMetrics` as a simple example.

```go
type Metrics interface {
  QueryDuration(duration time.Duration, query string)
  ConnectionCount(active, idle int32)
  ErrorCount(errorType string)
  CircuitStateChanged(state string)
}

// Wire it in
db, _ := norm.New(cfg, norm.WithMetrics(MyMetrics{}))
```

Example adapter (`ExpvarMetrics`) exposes counters under `/debug/vars` when the expvar handler is mounted.


