## Relations

Helpers for simple eager and lazy loading without heavy ORM mapping.

```go
// Eager load children for a slice of parents
var parents []*User
// ... fill parents
_ = norm.EagerLoadMany(ctx, db, parents, func(u *User) any { return u.ID }, "user_id", func(u *User, ps []*Profile) {
  // assign ps to a field or process
})

// Lazy load children for a single parent id
ps, _ := norm.LazyLoadMany[Profile](ctx, db, userID, "user_id")
```


