## UUID Primary Key

```go
type UUIDItem struct {
  ID   string `db:"id" norm:"primary_key,type:uuid,default:gen_random_uuid()"`
  Name string `db:"name"`
}

_ = db.AutoMigrate(&UUIDItem{})
```

Note: ensure `gen_random_uuid()` is available (e.g., enable `pgcrypto` extension).


