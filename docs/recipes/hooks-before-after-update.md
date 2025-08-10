## Hooks: BeforeUpdate / AfterUpdate

```go
func (u *User) BeforeUpdate(ctx context.Context) error {
  // validate changes
  return nil
}

func (u *User) AfterUpdate(ctx context.Context) error {
  // publish event
  return nil
}

_ = repo.Update(ctx, u)
```


