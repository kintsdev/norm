## Quickstart

Install and connect, run a health check, and auto-migrate a couple of models.

```go
package main

import (
    "context"
    "log"
    "time"

    "github.com/kintsdev/norm"
)

type User struct {
    ID        int64      `db:"id" norm:"primary_key,auto_increment"`
    Email     string     `db:"email" norm:"unique,not_null,index,varchar(255)"`
    Username  string     `db:"username" norm:"unique,not_null,varchar(50)"`
    Password  string     `db:"password" norm:"not_null,varchar(255)"`
    IsActive  bool       `db:"is_active" norm:"default:true"`
    CreatedAt time.Time  `db:"created_at" norm:"not_null,default:now()"`
    UpdatedAt time.Time  `db:"updated_at" norm:"not_null,default:now(),on_update:now()"`
    DeletedAt *time.Time `db:"deleted_at" norm:"index"`
}

func main() {
    ctx := context.Background()
    cfg := &norm.Config{Host: "localhost", Port: 5432, Database: "postgres", Username: "postgres", Password: "postgres"}
    db, err := norm.New(cfg)
    if err != nil { log.Fatal(err) }
    defer func() { _ = db.Close() }()

    if err := db.Health(ctx); err != nil { log.Fatal(err) }

    // Auto-migrate schema from model definitions
    if err := db.AutoMigrate(&User{}); err != nil { log.Fatal(err) }

    // Simple query
    var rows []map[string]any
    _ = db.Query().Table("users").Select("id", "email").Limit(5).Find(ctx, &rows)
}
```

Next: `guides/migrations.md`, `guides/query-builder.md`, `guides/repository.md`.


