## Retries

Configure transient error retries via `Config`.

```go
cfg.RetryAttempts = 3
cfg.RetryBackoff = 100 * time.Millisecond
db, _ := norm.New(cfg)
```

Notes:

- Retries apply to database operations performed through the client.
- Ensure idempotency for writes that may be retried.


