## Configuration

Construct `norm.Config` or use a connection string.

```go
cfg := &norm.Config{
  Host: "localhost",
  Port: 5432,
  Database: "postgres",
  Username: "postgres",
  Password: "postgres",
  SSLMode: "disable",
  MaxConnections: 10,
  MinConnections: 1,
  MaxConnLifetime: 30 * time.Minute,
  MaxConnIdleTime: 5 * time.Minute,
  HealthCheckPeriod: 30 * time.Second,
  ConnectTimeout: 5 * time.Second,
  ApplicationName: "myapp",
  ReadOnlyConnString: "host=replica dbname=postgres user=postgres password=postgres sslmode=disable",
  RetryAttempts: 3,
  RetryBackoff: 100 * time.Millisecond,
  StatementCacheCapacity: 256,
  // Circuit breaker
  CircuitBreakerEnabled: true,
  CircuitFailureThreshold: 5,
  CircuitOpenTimeout: 30 * time.Second,
  CircuitHalfOpenMaxCalls: 1,
}
db, err := norm.New(cfg)
```

Alternatively:

```go
db, err := norm.NewWithConnString("host=... port=... dbname=... user=... password=... sslmode=disable")
```

`ReadOnlyConnString` enables a read-replica pool. Reads are routed automatically; force with `QueryRead()` or `UseReadPool()` and override with `UsePrimary()`.


