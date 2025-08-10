## Rename Column (Auto-Migrate)

```go
type UserV1 struct { Username string `db:"username"` }
type UserV2 struct { Handle   string `db:"handle" norm:"rename:username"` }

// Run auto-migrate with the new model to rename the column safely.
_ = db.AutoMigrate(&UserV2{})
```


