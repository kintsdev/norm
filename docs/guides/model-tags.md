## Model Tags

Use `db:"..."` for column name and `norm:"..."` for ORM/migration metadata (legacy alias: `orm`).

Examples:

```go
type User struct {
  ID        int64      `db:"id" norm:"primary_key,auto_increment"`
  Email     string     `db:"email" norm:"unique,not_null,index,varchar(255)"`
  Username  string     `db:"username" norm:"unique,not_null,varchar(50)"`
  Password  string     `db:"password" norm:"not_null,varchar(255)"`
  IsActive  bool       `db:"is_active" norm:"default:true"`
  CreatedAt time.Time  `db:"created_at" norm:"not_null,default:now()"`
  UpdatedAt time.Time  `db:"updated_at" norm:"not_null,default:now(),on_update:now()"`
  DeletedAt *time.Time `db:"deleted_at" norm:"index"`
  Version   int64      `db:"version" norm:"version"`
}
```

Supported tokens (selection):

- **primary_key** (or `primary_key:group`)
- **auto_increment**
- **unique** (or `unique:group`, `unique_name:name`)
- **not_null** / `nullable`
- **default:value**
- **index** (or `index:name`, `using:btree|gin|hash`, `index_where:...`)
- **on_update:now()**
- **version** (optimistic locking)
- **fk:table(column)** (plus `fk_name:...`, `on_delete:...`, `on_update_fk:...`, `deferrable`, `initially_deferred`)
- **rename:old_name**
- **collate:...**
- **comment:...**
- **type:OVERRIDE** or inline overrides like `varchar(255)`, `numeric(10,2)`, `citext`


