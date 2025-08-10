## Soft Delete

Model with `deleted_at` column and use repository helpers.

```go
type User struct {
  ID        int64      `db:"id" norm:"primary_key,auto_increment"`
  // ...
  DeletedAt *time.Time `db:"deleted_at" norm:"index"`
}

repo := norm.NewRepository[User](db)
_ = repo.SoftDelete(ctx, id)
_ = repo.Restore(ctx, id)
// Default scope excludes deleted; use:
_, _ = repo.WithTrashed().FindOne(ctx, norm.Eq("id", id))
_, _ = repo.OnlyTrashed().FindOne(ctx, norm.Eq("id", id))
```


