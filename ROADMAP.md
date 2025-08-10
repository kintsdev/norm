## Kints-Norm Roadmap

This document tracks planned work. Items are grouped by area. Checked items are implemented and covered by e2e tests.

### Core & Stability

- [x] PGX v5 + pgxpool integration, context-based ops, graceful shutdown
- [x] Config with pooling, timeouts, statement cache capacity
- [x] Health checks (`SELECT 1`)
- [x] Retry attempts + backoff (exponential with jitter)
- [x] Circuit breaker (open/half-open/closed) with metrics

### Migration Engine

- [x] Struct tag parsing (`db`, `orm`) including PK, unique, not_null, default, index, FK, version
- [x] Create table, add column if not exists, indexes, foreign keys
- [x] Idempotency via checksum in `schema_migrations`
- [x] Transactional migrations with `pg_advisory_xact_lock`
- [x] Plan/preview API with diff against `information_schema`
- [x] Identifier quoting for DDL
- [x] Rename detection via `orm:"rename:old_name"` (safe plan)
- [x] Type and nullability change warnings + unsafe statements
- [x] Manual migrations: file-based SQL (Up/Down)
- [ ] Go-based migration helpers (functions)
- [x] Rollback: down-runner with safety gates
- [x] Drop/rename column plan (diff) with explicit opt-in guards (apply-time)
- [ ] Table drop/rename planning (explicit opt-in)
- [x] Index/constraint drop diffing (apply-time opt-in)
- [x] Detailed plan formatting (grouping by table, severity)

### Query Builder

- [x] Fluent `Select/Where/Join/OrderBy/Limit/Offset`
- [x] Raw with `?` to `$n` placeholder conversion
- [x] First/Last, Delete chain methods
- [x] Insert/Update with `RETURNING`, `ON CONFLICT DO UPDATE`
- [x] Condition DSL (`Eq/Ne/Gt/Ge/Lt/Le/In/And/Or`)
- [x] Keyset pagination (`After/Before`)
- [x] Struct ops: `InsertStruct`, `UpdateStructByPK`
- [x] Identifier-quoting helpers in builder API (safe column/table refs) (e2e covered)
- [x] Named parameters support (`WhereNamed`, `RawNamed`) (e2e covered)
- [x] Prebuilt common scopes (e.g., by date ranges) (e2e covered)

### Repository & Transactions

- [x] Generic CRUD (`Create/Update/Delete/Find/Count/Exists`)
- [x] Partial updates, bulk inserts (`CreateBatch`, copy support placeholder)
- [x] Soft delete with default scoping (`WithTrashed`, `OnlyTrashed`, `Restore`, `PurgeTrashed`)
- [x] Optimistic locking (`orm:"version"`)
- [x] Transactions (`TxManager`, transaction-bound QueryBuilder)
- [ ] Auto route read operations to read-replica pool; writes to primary
- [x] Upsert helpers in repository (e2e covered)
- [x] Eager/lazy loading helpers (e2e covered)

### Read/Write Splitting & Caching

- [x] Optional read pool via `ReadOnlyConnString` + `QueryRead()` (e2e covered)
- [x] Auto read routing + overrides (`UsePrimary`, `UseReadPool`) (e2e covered)
- [ ] Cache integration hooks (read-through/write-through)
- [ ] Cache invalidation on write/tx commit

### Observability & Logging

- [x] Metrics/Logger interfaces
- [x] Basic query duration metric calls in builder
- [ ] Structured logging with context fields, correlation IDs
- [ ] Slow query logging with threshold and parameter masking
- [ ] Built-in metrics adapter examples

### Security & Production

- [x] SQL injection safety via parameterization
- [ ] Secret providers for connection string (env manager, KMS)
- [ ] Audit logging hook points
- [ ] RLS helpers (session vars, `SET ROLE` helpers)

### Testing & Tooling

- [x] Makefile to run Postgres in Docker + e2e suite
- [x] Comprehensive e2e tests for migrations, CRUD, soft delete, tx, builder, pagination, DSL, struct ops
- [x] Migration diff tests (rename/type/nullability) and quoting
- [x] E2E tests for read pool via `QueryRead()`
- [ ] E2E tests for auto read routing + retry policies (idempotency cases)
- [ ] Lint and coverage targets; example CI workflow

### Nice-to-haves

- [ ] CLI: migration plan/apply/rollback
- [ ] Documentation site with full guides and recipes
