## Named Parameters

```go
var rows []map[string]any
_ = db.Query().Table("users").WhereNamed("id IN :ids", map[string]any{"ids": []int64{1,2,3}}).Find(ctx, &rows)

_ = db.Query().RawNamed("INSERT INTO t_tmp(x) VALUES(:x)", map[string]any{"x": 42}).Exec(ctx)
```


