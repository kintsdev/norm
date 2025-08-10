## Circuit Breaker

Enable in `Config` to protect against cascading failures.

```go
cfg.CircuitBreakerEnabled = true
cfg.CircuitFailureThreshold = 5
cfg.CircuitOpenTimeout = 30 * time.Second
cfg.CircuitHalfOpenMaxCalls = 1
```

State changes are exposed to `Metrics.CircuitStateChanged`. Open-state attempts return a connection error mapped to `ORMError` with code `ErrCodeConnection`.


