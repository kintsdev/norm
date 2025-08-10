# norm (Next ORM)

Production-ready, lightweight ORM and query builder for PostgreSQL on top of PGX v5. Ships with connection pooling, automatic migrations from struct tags, a fluent query builder, generic repository, soft delete, optimistic locking, transactions, read/write splitting, retry/backoff, a circuit breaker, and comprehensive e2e tests.

### Features

- Fast, reliable connections via PGX v5 (`pgxpool`)
- Flexible `Config`: pool limits, timeouts, statement cache, app name, etc.
- Auto-migration from struct tags: tables/columns/indexes/FKs, idempotent plan, transactional apply, rename diffs, type/nullability warnings
- Query builder: `Select/Where/Join/OrderBy/Limit/Offset`, `Raw`, `First/Last`, `Delete` (soft by default, `HardDelete()` to force hard), `INSERT ... RETURNING`, `ON CONFLICT DO UPDATE`
- Condition DSL: `Eq/Ne/Gt/Ge/Lt/Le/In/And/Or`, date helpers
- Keyset pagination: `After/Before`
- Repository: generic CRUD, bulk create, partial update, soft delete, scopes, optimistic locking
- Transactions: `TxManager`, transaction-bound QueryBuilder
- Read/Write splitting: optional read pool and transparent routing
- Retry: exponential backoff
- Circuit Breaker: optional open/half-open/closed with metrics hooks

Note: OpenTelemetry/Prometheus integrations are not included yet.

### Install

```bash
go get github.com/kintsdev/norm
```

### Quick Start

```go
package main

import (
    "context"
    "time"

    "github.com/kintsdev/norm"
)

type User struct {
    ID        int64      `db:"id" norm:"primary_key,auto_increment"`
    Email     string     `db:"email" norm:"unique,not_null,index,varchar(255)"`
    Username  string     `db:"username" norm:"unique,not_null,varchar(50)"`
    Password  string     `db:"password" norm:"not_null,varchar(255)"`
    IsActive  bool       `db:"is_active" norm:"default:true"`
    CreatedAt time.Time  `db:"created_at" norm:"not_null,default:now()"`
    UpdatedAt time.Time  `db:"updated_at" norm:"not_null,default:now(),on_update:now()"`
    DeletedAt *time.Time `db:"deleted_at" norm:"index"`
    Version   int64      `db:"version" norm:"version"`
}

func main() {
    cfg := &norm.Config{
        Host: "127.0.0.1", Port: 5432, Database: "postgres", Username: "postgres", Password: "postgres",
        SSLMode: "disable", StatementCacheCapacity: 256,
    }
    kn, _ := norm.New(cfg)
    defer kn.Close()

    // Auto-migrate schema from struct tags
    _ = kn.AutoMigrate(&User{})

    // Repository
    repo := norm.NewRepository[User](kn)
    _ = repo.Create(context.Background(), &User{Email: "u@example.com", Username: "u", Password: "x"})

    // Query builder
    var users []User
    _ = kn.Query().Table("users").Where("is_active = ?", true).OrderBy("id ASC").Limit(10).Find(context.Background(), &users)
}
```

### Struct Tags

- `db:"column_name"`: Column name; if empty, the field name is converted to snake_case.
- `norm:"..."`: Primary tag for schema/behavior (legacy `orm:"..."` still works as a fallback).

Supported `norm` tokens (mix and match, comma separated):

- Primary key: `primary_key`, composite via `primary_key:group`
- Auto-increment identity: `auto_increment`
- Unique: `unique`, composite via `unique:group`, optional index name via `unique_name:name`
- Indexing: `index`, `index:name`, index method `using:gin|btree|hash`, partial index `index_where:(expr)`
- Foreign keys: `fk:other_table(other_id)`, `fk_name:name`, actions `on_delete:cascade|restrict|set null|set default`, optional `deferrable`, `initially_deferred`
- Nullability: `not_null`, or explicit `nullable`
- Default: `default:<expr>` (e.g., `default:now()`)
- On update: `on_update:now()` (repository auto-sets NOW() on update for such columns)
- Version column for optimistic locking: `version` (treated as BIGINT)
- Rename diff: `rename:old_column`
- Collation: `collate:<name>`
- Comment: `comment:...`
- Type override: `type:decimal(20,8)` or direct types like `varchar(50)`, `text`, `timestamptz`, `numeric(10,2)`, `citext`
- Ignore field: `-` or `ignore` (excluded from migrations and insert/update helpers)

Examples:

```go
// Composite unique
Slug   string `db:"slug" norm:"not_null,unique:tenant_slug"`
Tenant int64  `db:"tenant_id" norm:"not_null,unique:tenant_slug,unique_name:uq_accounts_tenant_slug"`

// Partial index and method
Email  string `db:"email" norm:"index,using:gin,index_where:(deleted_at IS NULL)"`

// Decimal override
Amount float64 `db:"amount" norm:"type:decimal(20,8)"`

// FK with actions
UserID int64 `db:"user_id" norm:"not_null,fk:users(id),on_delete:cascade,fk_name:fk_posts_user"`
```

### Migrations

- Plan/preview: reads current schema via `information_schema`, builds a safe plan
- Creates tables/columns, composite indexes/uniques, and foreign keys (with actions)
- Rename-safe diffs: `ALTER TABLE ... RENAME COLUMN ...`
- Type/nullability changes produce warnings and unsafe statements
- Transactional apply with advisory lock
- Records checksums in `schema_migrations` (idempotent)

Manual migrations (file-based Up/Down) and rollback support exist with safety guards; see `migration` package and tests.

### Read/Write Splitting, Retry, Circuit Breaker

- If `Config.ReadOnlyConnString` is set, a read pool is opened and `Query()` routes read queries there automatically. Writes go to primary.
- Override per-query: `UsePrimary()` or `UseReadPool()`.
- Retry with `RetryAttempts` and `RetryBackoff` (exponential + jitter).
- Circuit breaker: `CircuitBreakerEnabled`, `CircuitFailureThreshold`, `CircuitOpenTimeout`, `CircuitHalfOpenMaxCalls`.

### Optional Cache Hooks

- Provide a cache via `WithCache(cache)` (e.g., a Redis adapter)
- Read-through: `Query().WithCacheKey(key, ttl).Find/First`
- Invalidation: `WithInvalidateKeys(keys...).Exec/Insert/Update/Delete`

### Testing

- Make targets spin up Postgres 17.5 in Docker and run e2e tests

```bash
make db-up
make test-e2e
make db-down
```

### Benchmarks

Micro and end-to-end benchmarks are included. Run micro (no DB required) or full (requires Postgres env like the e2e tests).

Run all benchmarks (micro + e2e):

```bash
go test -bench=. -benchmem -run=^$ ./...
```

Only micro (root package):

```bash
go test -bench=. -benchmem -run=^$
```

Examples (Apple M3, Go 1.22, local PG):

- Placeholder conversion and builder (ns–µs level)
  - `ConvertQMarksToPgPlaceholders`: ~250 ns/op, 208 B/op, 9 alloc/op
  - `ConvertNamedToPgPlaceholders` (scalars/reuse, slice expansion): ~390–690 ns/op
  - `StructMapper` (cached): ~9 ns/op, 0 alloc/op
  - `Build SELECT with JOINs`: ~1.1 µs/op

- E2E (depends on DB latency)
  - `FindPage` (COUNT + SELECT): ~0.3 ms/op
  - `Scan 100 rows`: ~0.25–0.30 ms/op
  - `CopyFrom(500 rows)`: ~0.08 ms/op
  - Single-row writes (Insert/Upsert/Tx): ~6–7 ms/op

Notes:
- Results vary by CPU, Go version, and Postgres settings; numbers above are indicative.
- Micro benchmarks live in `bench_test.go`, e2e in `e2e/bench_e2e_test.go`.

### License

MIT
