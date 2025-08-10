## Date Range Queries

```go
var rows []map[string]any
_ = db.Query().Table("orders").WhereCond(norm.DateRange("created_at", from, to)).Find(ctx, &rows)
_ = db.Query().Table("orders").WhereCond(norm.OnDate("created_at", day)).Find(ctx, &rows)
```


