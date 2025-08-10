## Foreign Key Actions

```go
type Parent struct { ID int64 `db:"id" norm:"primary_key,auto_increment"` }
type Child struct  { ID int64 `db:"id" norm:"primary_key,auto_increment"`; ParentID int64 `db:"parent_id" norm:"not_null,fk:parents(id),on_delete:cascade,fk_name:fk_child_parent"` }

_ = db.AutoMigrate(&Parent{}, &Child{})
```

Deleting a parent cascades to `childs` due to `on_delete:cascade`.


