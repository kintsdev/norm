## Composite Unique Constraint

```go
type Account struct {
  ID       int64  `db:"id" norm:"primary_key,auto_increment"`
  TenantID int64  `db:"tenant_id" norm:"not_null,unique:tenant_slug"`
  Slug     string `db:"slug" norm:"not_null,unique:tenant_slug,unique_name:uq_accounts_tenant_slug"`
}

_ = db.AutoMigrate(&Account{})
```


