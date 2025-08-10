## kints-norm

Production-ready, PGX v5 tabanlı hafif ORM ve query builder (PostgreSQL). Bağlantı havuzu, otomatik migration, fluent query builder, generic repository, soft delete, optimistic locking, transaction yönetimi, read/write splitting, retry/backoff, ve kapsamlı e2e testleri ile gelir.

### Özellikler
- PGX v5 (`pgxpool`) ile hızlı ve güvenilir bağlantı yönetimi
- Config ile esnek ayarlar: pool sınırları, timeouts, statement cache, app name vb.
- Otomatik migration: struct tag’lerden tablo/kolon/index/FK üretimi, idempotent plan, transactional apply, rename diff, type/nullability uyarıları
- Query builder: `Select/Where/Join/OrderBy/Limit/Offset`, `Raw`, `First/Last`, `Delete`, `INSERT ... RETURNING`, `ON CONFLICT DO UPDATE`
- Condition DSL: `Eq/Ne/Gt/Ge/Lt/Le/In/And/Or`
- Keyset pagination: `After/Before`
- Repository: generic CRUD, bulk create, partial update, soft delete, scopes, optimistic locking
- Transaction yönetimi: `TxManager`, transaction-bound QueryBuilder
- Read/Write splitting: read pool opsiyonel, fallback
- Retry: exponential backoff

Not: OpenTelemetry/Prometheus entegrasyonları şimdilik kapsam dışıdır.

### Kurulum

```bash
go get kints-norm
```

Go mod klasörünüzde `kints-norm` import edin.

### Hızlı Başlangıç

```go
package main

import (
    kintsnorm "kints-norm"
)

type User struct {
    ID        int64      `db:"id" orm:"primary_key,auto_increment"`
    Email     string     `db:"email" orm:"unique,not_null,index,varchar(255)"`
    Username  string     `db:"username" orm:"unique,not_null,varchar(50)"`
    Password  string     `db:"password" orm:"not_null,varchar(255)"`
    IsActive  bool       `db:"is_active" orm:"default:true"`
    CreatedAt time.Time  `db:"created_at" orm:"not_null,default:now()"`
    UpdatedAt time.Time  `db:"updated_at" orm:"not_null,default:now(),on_update:now()"`
    DeletedAt *time.Time `db:"deleted_at" orm:"index"`
    Version   int64      `db:"version" orm:"version"`
}

func main() {
    cfg := &kintsnorm.Config{
        Host: "127.0.0.1", Port: 5432, Database: "postgres", User: "postgres", Password: "postgres",
        SSLMode: "disable", StatementCacheCapacity: 256,
    }
    kn, _ := kintsnorm.New(cfg)
    defer kn.Close()

    // migrate
    _ = kn.AutoMigrate(&User{})

    // repository
    repo := kintsnorm.NewRepository[User](kn)
    _ = repo.Create(context.Background(), &User{Email: "u@example.com", Username: "u", Password: "x"})

    // query builder
    var users []User
    _ = kn.Query().Table("users").Where("is_active = ?", true).OrderBy("id ASC").Limit(10).Find(context.Background(), &users)
}
```

### Struct Tag’leri (özet)
- `db:"column_name"` kolon adını belirler; boşsa snake_case kullanılır
- `orm:"primary_key"`, `auto_increment`, `unique`, `not_null`, `default:now()`, `index`, `on_update:now()`, `version`
- `orm:"fk:other_table(other_id)"` yabancı anahtar
- `orm:"rename:old_column"` kolon rename diff’i için
- Tip override: `varchar(50)`, `text`, `timestamptz`, vb.

### Migration
- Plan/preview: mevcut şemayı `information_schema` üzerinden okuyup güvenli plan üretir
- `CREATE TABLE IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`, index/FK oluşturma
- Rename’ler için güvenli `ALTER TABLE ... RENAME COLUMN ...`
- Tip ve nullability değişimleri için Warnings ve UnsafeStatements
- Uygulama transactional ve advisory lock ile
- `schema_migrations` tablosunda checksum ile idempotent kayıt

Yakında: manuel (dosya tabanlı) Up/Down migrasyonları, rollback desteği, drop/rename table için guard’lar.

### Read/Write Splitting ve Retry
- `Config.ReadOnlyConnString` verilirse read pool açılır; `QueryRead()` ile kullanılır
- İleride: read operasyonlarını otomatik olarak read pool’a yönlendirme
- Retry: `RetryAttempts` ve `RetryBackoff` (exponential + jitter)

### Testler
- `Makefile` ile Postgres 17.5 docker ve kapsamlı e2e testleri
- Testler CRUD, soft delete, transaction, query builder, pagination, DSL, struct ops, migration diff/quoting’i kapsar

Çalıştırma:

```bash
make db-up
make test-e2e
make db-down
```

### Yol Haritası
Detaylı plan için `ROADMAP.md` dosyasına bakın.

### Lisans
MIT


