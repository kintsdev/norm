## Errors

Norm wraps failures into `ORMError` with a high-level `Code`, a human `Message`, and the original `Internal` error. SQL `Query` and `Args` are attached when available.

Error codes and typical sources:

- ErrCodeConnection: connection issues, circuit breaker open
- ErrCodeNotFound: no rows for `First`/`Last`, repository `GetByID`/`FindOne`
- ErrCodeDuplicate: unique constraint violations (PG 23505)
- ErrCodeConstraint: FK/check/not-null violations (PG 23503/23514/23502)
- ErrCodeTransaction: serialization failures (PG 40001), context canceled, tx errors
- ErrCodeMigration: migration-specific errors
- ErrCodeValidation: bad inputs (wrong dest type, missing OrderBy for Last, etc.)
 - ErrCodeInvalidColumn: unknown/undefined column (PG 42703 or API-level column checks)

PostgreSQL mapping (subset):

- 23505 → ErrCodeDuplicate
- 23503/23514/23502 → ErrCodeConstraint
- 40001 → ErrCodeTransaction

Special cases:

- Circuit breaker open → ErrCodeConnection
- Context canceled → ErrCodeTransaction

Pattern: handling by code

```go
if err != nil {
  var oe *norm.ORMError
  if errors.As(err, &oe) {
    switch oe.Code {
    case norm.ErrCodeNotFound:
      // 404-like path
    case norm.ErrCodeDuplicate:
      // return 409
    case norm.ErrCodeConstraint:
      // 422 validation
    case norm.ErrCodeInvalidColumn:
      // 400-level invalid field/column
    case norm.ErrCodeConnection:
      // 503 retryable
    }
  }
}
```

Inspecting the original pg error:

```go
var pgErr *pgconn.PgError
if errors.As(err, &pgErr) {
  // pgErr.Code, pgErr.Detail, pgErr.ConstraintName
}
```

NotFound behavior:

- `QueryBuilder.First/Last` returns `ErrCodeNotFound` when no rows.
- Repository `GetByID/FindOne` returns `ErrCodeNotFound` when not found.

Validation errors examples:

- `Last` without `OrderBy`
- `Exec` called without prior `Raw`
- Wrong dest type for `Find/ExecInsert/ExecUpdate` (expecting `[]map[string]any` for RETURNING)

Observability:

- When logging is enabled, fields include `sql`, `args`, and `stmt` (inlined SQL). Use `WithLogParameterMasking(true)` to hide args and avoid inlining.


