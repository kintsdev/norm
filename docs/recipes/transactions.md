## Transactions

Two styles: scoped function or manual begin/commit.

```go
// Scoped
_ = db.Tx().WithTransaction(ctx, func(tx norm.Transaction) error {
    tr := norm.NewRepositoryWithExecutor[User](db, tx.Exec())
    return tr.Create(ctx, &User{Email: "x@y", Username: "x", Password: "pw"})
})

// Manual
tx, err := db.Tx().BeginTx(ctx, &norm.TxOptions{})
if err == nil {
    tr := norm.NewRepositoryWithExecutor[User](db, tx.Exec())
    _ = tr.Create(ctx, &User{Email: "y@y", Username: "y", Password: "pw"})
    _ = tx.Commit(ctx)
}
```


