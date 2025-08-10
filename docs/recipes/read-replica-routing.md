## Read-Replica Routing

Config:

```go
cfg.ReadOnlyConnString = "host=replica dbname=postgres user=postgres password=postgres sslmode=disable"
db, _ := norm.New(cfg)
```

Usage:

```go
_ = db.QueryRead().Table("users").Limit(1).Find(ctx, &rows)   // force read pool
_ = db.Query().UseReadPool().Table("users").Limit(1).Find(ctx, &rows)
_ = db.Query().UsePrimary().Table("users").Limit(1).Find(ctx, &rows)
```


