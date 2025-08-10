## Repository

Generic CRUD with soft delete, pagination, upsert, and bulk insert via CopyFrom.

```go
type User struct { /* fields with db/norm tags */ }

repo := norm.NewRepository[User](db)
// Create
_ = repo.Create(ctx, &User{Email: "u@example.com", Username: "u", Password: "pw"})
// Batch
_ = repo.CreateBatch(ctx, []*User{{Email: "a@x", Username: "a", Password: "pw"}})
// Read
u, _ := repo.FindOne(ctx, norm.Eq("email", "u@example.com"))
// Update
if u != nil { _ = repo.Update(ctx, u) }
// Partial update
_ = repo.UpdatePartial(ctx, 1, map[string]any{"username": "u1"})
// Count/Exists
_, _ = repo.Count(ctx, norm.Eq("is_active", true))
_, _ = repo.Exists(ctx, norm.Eq("email", "u@example.com"))
// Pagination
page, _ := repo.FindPage(ctx, norm.PageRequest{Limit: 10, Offset: 0, OrderBy: "id ASC"})
_ = page
// Soft delete helpers
_ = repo.SoftDelete(ctx, 1)
_ = repo.Restore(ctx, 1)
_, _ = repo.SoftDeleteAll(ctx)
_, _ = repo.PurgeTrashed(ctx)
// Scopes
_, _ = repo.WithTrashed().FindOne(ctx, norm.Eq("id", 1))
_, _ = repo.OnlyTrashed().FindOne(ctx, norm.Eq("id", 1))
// Upsert
_ = repo.Upsert(ctx, &User{Email: "u@example.com", Username: "u2", Password: "pw"}, []string{"email"}, []string{"username"})
// Bulk insert
_, _ = repo.CreateCopyFrom(ctx, []*User{{Email: "b@x", Username: "b", Password: "pw"}}, "email", "username", "password")
```


