## Query Builder

Select:

```go
var rows []map[string]any
_ = db.Query().Table("users").Select("id", "email").Where("is_active = ?", true).OrderBy("id ASC").Limit(10).Find(ctx, &rows)
```

Named params and IN:

```go
_ = db.Query().Table("users").WhereNamed("id IN :ids", map[string]any{"ids": []int64{1,2,3}}).Find(ctx, &rows)
```

Conditions DSL:

```go
_ = db.Query().Table("users").WhereCond(norm.And(norm.Eq("is_active", true), norm.Ne("email", "x@example.com"))).Find(ctx, &rows)
```

Insert/Update/Delete with RETURNING:

```go
var inserted []map[string]any
_, _ = db.Query().Table("profiles").Insert("user_id", "bio").Values(1, "hello").Returning("id").ExecInsert(ctx, &inserted)

var updated []map[string]any
_, _ = db.Query().Table("users").Set("username = ?", "u2").Where("email = ?", "u@example.com").Returning("id").ExecUpdate(ctx, &updated)

_, _ = db.Query().Table("profiles").Where("user_id = ?", 1).HardDelete().Delete(ctx)
```

Keyset pagination helpers:

```go
_ = db.Query().Table("users").OrderBy("id ASC").After("id", 123).Limit(20).Find(ctx, &rows)
```

Read routing:

```go
_ = db.QueryRead().Table("users").Limit(1).Find(ctx, &rows)
_ = db.Query().UseReadPool().Table("users").Limit(1).Find(ctx, &rows)
_ = db.Query().UsePrimary().Table("users").Limit(1).Find(ctx, &rows)
```

Soft delete scoping for `Model(...)` queries (auto `deleted_at IS NULL`):

```go
// When using Model(&User{}), a default soft-delete filter is applied if the model has a deleted_at column.
_ = db.Query().Model(&User{}).Find(ctx, &rows)               // default excludes deleted
_ = db.Query().Model(&User{}).WithTrashed().Find(ctx, &rows) // include deleted
_ = db.Query().Model(&User{}).OnlyTrashed().Find(ctx, &rows) // only deleted
```

Identifier quoting helpers:

```go
_ = db.Query().TableQ("public.users").SelectQ("users.id", "users.email").Find(ctx, &rows)
_ = db.Query().SelectQI("strange.name").Find(ctx, &rows)
```


