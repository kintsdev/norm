## Optimistic Locking

Use `norm:"version"` to auto-increment version on update and detect write conflicts.

```go
type User struct {
  ID      int64 `db:"id" norm:"primary_key,auto_increment"`
  Version int64 `db:"version" norm:"version"`
}

// First read user into u1 and u2, update both; second update will fail with conflict
_ = repo.Update(ctx, u1)
if err := repo.Update(ctx, u2); err != nil { /* conflict */ }
```


