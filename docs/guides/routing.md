## Read/Write Routing

If `ReadOnlyConnString` is set in config, reads are routed to the read pool automatically for builder-based reads.

- Force read pool: `db.QueryRead()` or `db.Query().UseReadPool()`
- Force primary: `db.Query().UsePrimary()`

Writes (Exec/Insert/Update/Delete) go to primary.


