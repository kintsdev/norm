## Hooks: BeforeSoftDelete / AfterSoftDelete

```go
func (u *User) BeforeSoftDelete(ctx context.Context, id any) error { return nil }
func (u *User) AfterSoftDelete(ctx context.Context, id any) error  { return nil }

_ = repo.SoftDelete(ctx, 1)
```


