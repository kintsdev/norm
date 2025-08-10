## Keyset Pagination

Requires a deterministic order. Use `After`/`Before` with the ordered column value.

```go
var rows []map[string]any
// fetch first page
_ = db.Query().Table("users").OrderBy("id ASC").Limit(10).Find(ctx, &rows)
// then use last seen id as cursor
_ = db.Query().Table("users").OrderBy("id ASC").After("id", lastID).Limit(10).Find(ctx, &rows)
```


