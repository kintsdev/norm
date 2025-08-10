## Partial Index

```go
type PartialIdx struct {
  ID        int64      `db:"id" norm:"primary_key,auto_increment"`
  Email     string     `db:"email" norm:"index,using:gin,index_where:(deleted_at IS NULL)"`
  DeletedAt *time.Time `db:"deleted_at"`
}

_ = db.AutoMigrate(&PartialIdx{})
```


