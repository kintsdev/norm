## Upsert

Repository API:

```go
_ = repo.Upsert(ctx, &User{Email: "u@example.com", Username: "u2", Password: "pw"}, []string{"email"}, []string{"username"})
```

Query builder:

```go
_, _ = db.Query().Table("users").
  Insert("email", "username", "password").
  Values("u@example.com", "u2", "pw").
  OnConflict("email").
  DoUpdateSet("username = ?", "u2").
  ExecInsert(ctx, nil)
```


