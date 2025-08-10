## Bulk Insert (CopyFrom)

High-throughput bulk insert using pgx `CopyFrom` via repository.

```go
count, err := repo.CreateCopyFrom(ctx,
  []*User{{Email: "b1@example.com", Username: "b1", Password: "pw"}, {Email: "b2@example.com", Username: "b2", Password: "pw"}},
  "email", "username", "password", "is_active",
)
```

Notes:

- Requires pool executor (works with repository created from the client; not within a transaction-bound executor).


