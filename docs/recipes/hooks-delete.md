## Hooks: BeforeDelete / AfterDelete

```go
func (u *User) BeforeDelete(ctx context.Context, id any) error {
  // guard deletes
  return nil
}
func (u *User) AfterDelete(ctx context.Context, id any) error {
  // cleanup
  return nil
}

_ = repo.Delete(ctx, 1)
```


