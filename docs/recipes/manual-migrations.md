## Manual SQL Migrations

Files: `migrations/0001_init.up.sql`, `migrations/0001_init.down.sql`

```go
_ = db.MigrateUpDir(ctx, "./migrations")
_ = db.MigrateDownDir(ctx, "./migrations", 1)
db.SetManualMigrationOptions(migration.ManualOptions{AllowTableDrop: false, AllowColumnDrop: false})
```


