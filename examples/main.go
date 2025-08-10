package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/kintsdev/norm"
	"github.com/kintsdev/norm/migration"
)

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

type Profile struct {
	ID        int64     `db:"id" norm:"primary_key,auto_increment"`
	UserID    int64     `db:"user_id" norm:"index,not_null"`
	Bio       string    `db:"bio" norm:"varchar(255)"`
	CreatedAt time.Time `db:"created_at" norm:"not_null,default:now()"`
}

func main() {
	ctx := context.Background()
	cfg := &norm.Config{
		Host:                   "localhost",
		Port:                   5432,
		Database:               "postgres",
		Username:               "postgres",
		Password:               "postgres",
		MaxConnections:         5,
		MinConnections:         1,
		StatementCacheCapacity: 256,
		// optional: enable circuit breaker
		CircuitBreakerEnabled: true,
		// Retry example
		RetryAttempts: 3,
		RetryBackoff:  100 * time.Millisecond,
	}

	db, err := norm.New(cfg, norm.WithCache(norm.NoopCache{}), norm.WithLogger(norm.NoopLogger{}), norm.WithMetrics(norm.NoopMetrics{}))
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// Health
	if err := db.Health(ctx); err != nil {
		log.Fatal("health:", err)
	}

	// Auto-migrate models
	if err := db.AutoMigrate(&User{}, &Profile{}); err != nil {
		log.Fatal("automigrate:", err)
	}
	// AutoMigrate with options (no destructive ops by default)
	if err := db.AutoMigrateWithOptions(ctx, migration.ApplyOptions{}, &User{}, &Profile{}); err != nil {
		log.Fatal("automigrate opts:", err)
	}
	// Manual migration safety options (for file-based down migrations)
	db.SetManualMigrationOptions(migration.ManualOptions{AllowTableDrop: false, AllowColumnDrop: false})

	// Demonstrate getting a migration plan summary
	mig := migration.NewMigrator(db.Pool())
	if plan, err := mig.Plan(ctx, &User{}, &Profile{}); err == nil {
		_ = migration.FormatPlan(plan) // format (ignored output)
	}

	// Quote utilities
	_ = norm.QuoteIdentifier("strange\"name")

	// Clean start for demo
	_, _ = db.Pool().Exec(ctx, "TRUNCATE users RESTART IDENTITY CASCADE")
	_, _ = db.Pool().Exec(ctx, "TRUNCATE profiles RESTART IDENTITY CASCADE")

	// Repository usage
	repo := norm.NewRepository[User](db)
	// Create single
	_ = repo.Create(ctx, &User{Email: "u1@example.com", Username: "u1", Password: "pw", IsActive: true})
	_ = repo.Create(ctx, &User{Email: "u2@example.com", Username: "u2", Password: "pw", IsActive: true})
	// Batch
	_ = repo.CreateBatch(ctx, []*User{{Email: "u3@example.com", Username: "u3", Password: "pw"}, {Email: "u4@example.com", Username: "u4", Password: "pw"}})

	// Find/FindOne with Condition DSL
	users, _ := repo.Find(ctx, norm.And(norm.Eq("is_active", true), norm.Or(norm.Eq("username", "u1"), norm.Eq("username", "u2"))))
	log.Printf("found active u1/u2: %d", len(users))
	one, _ := repo.FindOne(ctx, norm.Eq("email", "u1@example.com"))
	if one != nil {
		// Update and partial update
		one.Password = "newpw"
		_ = repo.Update(ctx, one)
		_ = repo.UpdatePartial(ctx, one.ID, map[string]any{"username": "u1x"})
	}
	// Count/Exists
	c, _ := repo.Count(ctx, norm.Eq("is_active", true))
	ex, _ := repo.Exists(ctx, norm.Eq("email", "u2@example.com"))
	log.Printf("count active=%d exists u2=%v", c, ex)

	// Pagination
	page, _ := repo.FindPage(ctx, norm.PageRequest{Limit: 2, Offset: 0, OrderBy: "id ASC"}, norm.Eq("is_active", true))
	log.Printf("page items=%d total=%d", len(page.Items), page.Total)

	// Soft delete, scopes, restore and purge
	if one != nil {
		_ = repo.SoftDelete(ctx, one.ID)
		_, _ = repo.WithTrashed().FindOne(ctx, norm.Eq("id", one.ID))
		_, _ = repo.OnlyTrashed().FindOne(ctx, norm.Eq("id", one.ID))
		_ = repo.Restore(ctx, one.ID)
	}
	_, _ = repo.SoftDeleteAll(ctx)
	_, _ = repo.PurgeTrashed(ctx)

	// Upsert
	_ = repo.Upsert(ctx, &User{Email: "u2@example.com", Username: "u2_up", Password: "pw"}, []string{"email"}, []string{"username"})

	// CopyFrom bulk insert
	_, _ = repo.CreateCopyFrom(ctx, []*User{{Email: "b1@example.com", Username: "b1", Password: "pw"}, {Email: "b2@example.com", Username: "b2", Password: "pw"}}, "email", "username", "password", "is_active")

	// Query builder - basic select
	var rows []map[string]any
	_ = db.Query().Table("users").Select("id", "email").Where("is_active = ?", true).OrderBy("id ASC").Limit(5).Find(ctx, &rows)
	log.Printf("users rows=%d", len(rows))

	// Query builder - named params and IN
	_ = db.Query().Table("users").WhereNamed("id IN :ids", map[string]any{"ids": []int64{1, 2, 3}}).Find(ctx, &rows)

	// Query builder - conditions API
	_ = db.Query().Table("users").WhereCond(norm.And(norm.Between("id", 1, 100), norm.Ne("email", "nobody@example.com"))).Find(ctx, &rows)

	// Keyset pagination helpers (requires order)
	var firstIDs []map[string]any
	_ = db.Query().Table("users").Select("id").OrderBy("id ASC").Limit(1).Find(ctx, &firstIDs)
	if len(firstIDs) > 0 {
		after := firstIDs[0]["id"]
		_ = db.Query().Table("users").OrderBy("id ASC").After("id", after).Limit(5).Find(ctx, &rows)
	}

	// Raw queries and Exec
	_ = db.Query().Raw("CREATE TEMP TABLE IF NOT EXISTS t_tmp(x int)").Exec(ctx)
	_ = db.Query().RawNamed("INSERT INTO t_tmp(x) VALUES(:x)", map[string]any{"x": 42}).Exec(ctx)

	// Insert builder with RETURNING
	var inserted []map[string]any
	_, _ = db.Query().Table("profiles").Insert("user_id", "bio").Values(1, "hello").Returning("id", "user_id").ExecInsert(ctx, &inserted)
	// Upsert via insert builder
	_, _ = db.Query().Table("users").Insert("email", "username", "password").Values("u2@example.com", "u2_up2", "pw").OnConflict("email").DoUpdateSet("username = ?", "u2_up2").ExecInsert(ctx, nil)

	// Update builder with RETURNING
	var updated []map[string]any
	_, _ = db.Query().Table("users").Set("username = ?", "u2_final").Where("email = ?", "u2@example.com").Returning("id", "username").ExecUpdate(ctx, &updated)

	// Delete builder
	_, _ = db.Query().Table("profiles").Where("user_id = ?", 1).Delete(ctx)

	// Struct helpers
	_, _ = db.Query().Table("profiles").InsertStruct(ctx, &Profile{UserID: 1, Bio: "bio2"})
	_, _ = db.Query().Table("profiles").UpdateStructByPK(ctx, &Profile{ID: 1, UserID: 1, Bio: "bio3"}, "id")

	// Cache hooks (NoopCache used)
	_ = db.Query().Table("users").WithCacheKey("users:first", 10*time.Second).Limit(1).Find(ctx, &rows)
	_ = db.Query().Raw("UPDATE users SET updated_at = NOW() WHERE id = 1").WithInvalidateKeys("users:first").Exec(ctx)

	// Read routing helpers
	_ = db.QueryRead().Table("users").Limit(1).Find(ctx, &rows) // uses read pool if configured
	_ = db.Query().UseReadPool().Table("users").Limit(1).Find(ctx, &rows)
	_ = db.Query().UsePrimary().Table("users").Limit(1).Find(ctx, &rows)

	// Transactions: WithTransaction (rollback example)
	_ = db.Tx().WithTransaction(ctx, func(tx norm.Transaction) error {
		tr := norm.NewRepositoryWithExecutor[User](db, tx.Exec())
		_ = tr.Create(ctx, &User{Email: "tx_rollback@example.com", Username: "txr", Password: "pw"})
		return fmt.Errorf("force rollback")
	})
	// Transactions: manual begin/commit
	tx, err := db.Tx().BeginTx(ctx, &norm.TxOptions{})
	if err == nil {
		tr := norm.NewRepositoryWithExecutor[User](db, tx.Exec())
		_ = tr.Create(ctx, &User{Email: "tx_commit@example.com", Username: "txc", Password: "pw"})
		_ = tx.Commit(ctx)
	}

	// Relations: Lazy/Eager load
	// ensure some data
	_ = repo.Create(ctx, &User{Email: "rel@example.com", Username: "rel", Password: "pw"})
	_, _ = db.Query().Table("profiles").Insert("user_id", "bio").Values(1, "p1").ExecInsert(ctx, nil)
	var parents []*User
	ptmp, _ := repo.Find(ctx)
	for _, p := range ptmp {
		parents = append(parents, p)
	}
	_ = norm.EagerLoadMany(ctx, db, parents, func(u *User) any { return u.ID }, "user_id", func(u *User, ps []*Profile) {
		// just demonstrate wiring
		_ = ps
	})
	_, _ = norm.LazyLoadMany[Profile](ctx, db, 1, "user_id")

	// Simple sanity query
	var sys []map[string]any
	_ = db.Query().Table("pg_catalog.pg_tables").Select("schemaname", "tablename").Limit(3).Find(ctx, &sys)
	log.Printf("system tables sample: %d", len(sys))
}
