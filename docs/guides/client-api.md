## Client API

Create and manage the `KintsNorm` client and access core entry points.

Construction:

```go
db, err := norm.New(&norm.Config{ /* ... */ })
db, err := norm.NewWithConnString("host=... port=... dbname=... user=... password=... sslmode=disable")
```

Lifecycle & health:

```go
_ = db.Health(ctx) // SELECT 1
_ = db.Close()     // close pools
```

Pools:

```go
primary := db.Pool()   // *pgxpool.Pool
read := db.ReadPool()  // *pgxpool.Pool (read-replica if configured; otherwise primary)
```

Query entry points:

```go
qb := db.Query()        // QueryBuilder (auto read-routing if read pool configured)
qb = db.QueryRead()     // force read pool for reads
qb = db.Model(&User{})  // infer table from model type
```

Transactions:

```go
txm := db.Tx()
_ = txm.WithTransaction(ctx, func(tx norm.Transaction) error { /* ... */ return nil })
tx, _ := txm.BeginTx(ctx, &norm.TxOptions{})
_ = tx.Commit(ctx)
```

Migrations:

```go
_ = db.AutoMigrate(&User{}, &Profile{})
_ = db.AutoMigrateWithOptions(ctx, migration.ApplyOptions{AllowDropColumns: true}, &User{})
_ = db.MigrateUpDir(ctx, "./migrations")
_ = db.MigrateDownDir(ctx, "./migrations", 1)
db.SetManualMigrationOptions(migration.ManualOptions{AllowTableDrop: false, AllowColumnDrop: false})
```


