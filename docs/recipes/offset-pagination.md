## Offset Pagination

Using repository helper:

```go
page, _ := repo.FindPage(ctx, norm.PageRequest{Limit: 20, Offset: 40, OrderBy: "id ASC"})
fmt.Println(page.Items, page.Total)
```

Using builder:

```go
var rows []map[string]any
_ = db.Query().Table("users").OrderBy("id ASC").Limit(20).Offset(40).Find(ctx, &rows)
```


