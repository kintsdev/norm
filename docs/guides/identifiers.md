## Identifiers & Quoting

Use quoting helpers when identifiers may collide with reserved words or contain special chars.

```go
// Quote a single identifier
q := norm.QuoteIdentifier("strange\"name") // => "strange""name"

// Table/column quoting in builder
_ = db.Query().TableQ("public.users").SelectQ("users.id", "users.email").Find(ctx, &rows)
_ = db.Query().SelectQI("weird.column").Find(ctx, &rows)
```


