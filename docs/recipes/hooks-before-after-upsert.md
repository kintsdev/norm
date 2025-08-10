## Hooks: BeforeUpsert / AfterUpsert

```go
func (u *User) BeforeUpsert(ctx context.Context) error { return nil }
func (u *User) AfterUpsert(ctx context.Context) error  { return nil }

_ = repo.Upsert(ctx, u, []string{"email"}, []string{"username"})
```


