## Hooks: BeforeRestore / AfterRestore, BeforePurgeTrashed / AfterPurgeTrashed

```go
func (u *User) BeforeRestore(ctx context.Context, id any) error { return nil }
func (u *User) AfterRestore(ctx context.Context, id any) error  { return nil }

func (u *User) BeforePurgeTrashed(ctx context.Context) error                 { return nil }
func (u *User) AfterPurgeTrashed(ctx context.Context, affected int64) error { return nil }

_ = repo.Restore(ctx, 1)
_, _ = repo.PurgeTrashed(ctx)
```


