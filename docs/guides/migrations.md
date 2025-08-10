## Migrations

Two ways to manage schema:

- Programmatic: `kn.AutoMigrate(models...)` and `AutoMigrateWithOptions` for controlled drops.
- File-based: `MigrateUpDir`/`MigrateDownDir` for `*.up.sql`/`*.down.sql` files.

Programmatic example:

```go
// Given models with `db` and `norm` tags
if err := db.AutoMigrate(&User{}, &Profile{}); err != nil { /* handle */ }

// Allow drops (careful):
if err := db.AutoMigrateWithOptions(ctx, migration.ApplyOptions{AllowDropColumns: true}, &User{}); err != nil { /* handle */ }
```

File-based example:

```go
// ./migrations/0001_init.up.sql, 0001_init.down.sql
if err := db.MigrateUpDir(ctx, "./migrations"); err != nil { /* handle */ }
// Roll back last N
if err := db.MigrateDownDir(ctx, "./migrations", 1); err != nil { /* handle */ }

// Safety gates for manual down migrations
db.SetManualMigrationOptions(migration.ManualOptions{AllowTableDrop: false, AllowColumnDrop: false})
```

Preview a plan:

```go
mig := migration.NewMigrator(db.Pool())
plan, err := mig.Plan(ctx, &User{}, &Profile{})
_ = plan
```


