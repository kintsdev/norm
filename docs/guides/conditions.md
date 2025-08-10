## Conditions DSL

Build typed conditions and compose with `And`/`Or`.

```go
cond := norm.And(
  norm.Eq("is_active", true),
  norm.Or(norm.Gt("id", 10), norm.In("id", []any{1,2,3})),
)
var rows []map[string]any
_ = db.Query().Table("users").WhereCond(cond).Find(ctx, &rows)

// Date helpers
_ = db.Query().Table("users").WhereCond(norm.DateRange("created_at", from, to)).Find(ctx, &rows)
_ = db.Query().Table("users").WhereCond(norm.OnDate("created_at", day)).Find(ctx, &rows)
```


