## Model Hooks

Implement optional interfaces on your model to run logic before/after lifecycle events.

Interfaces:

- `BeforeCreate` / `AfterCreate`
- `BeforeUpdate` / `AfterUpdate`
- `BeforeUpsert` / `AfterUpsert`
- `BeforeDelete` / `AfterDelete`
- `BeforeSoftDelete` / `AfterSoftDelete`
- `BeforeRestore` / `AfterRestore`
- `BeforePurgeTrashed` / `AfterPurgeTrashed`

Example:

```go
func (u *User) BeforeCreate(ctx context.Context) error {
  // validate or set defaults
  return nil
}

func (u *User) AfterUpdate(ctx context.Context) error {
  // emit event
  return nil
}
```


