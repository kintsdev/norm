## Hooks: BeforeCreate / AfterCreate

Implement on your model to run logic around insert.

```go
type User struct {
  ID    int64  `db:"id" norm:"primary_key,auto_increment"`
  Email string `db:"email"`
}

func (u *User) BeforeCreate(ctx context.Context) error {
  if u.Email == "" { return fmt.Errorf("email required") }
  return nil
}

func (u *User) AfterCreate(ctx context.Context) error {
  // emit event or warm cache
  return nil
}

repo := norm.NewRepository[User](db)
_ = repo.Create(ctx, &User{Email: "u@example.com"})
```


