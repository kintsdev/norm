package e2e

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	kintsnorm "github.com/kintsdev/norm"
	migration "github.com/kintsdev/norm/migration"
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

// Model without soft delete column to validate errors
type NoSoft struct {
	ID        int64     `db:"id" norm:"primary_key,auto_increment"`
	Name      string    `db:"name" norm:"not_null,varchar(100)"`
	CreatedAt time.Time `db:"created_at" norm:"not_null,default:now()"`
}

// FKPost -> fk_posts (has FK to users)
type FKPost struct {
	ID        int64     `db:"id" norm:"primary_key,auto_increment"`
	UserID    int64     `db:"user_id" norm:"not_null,fk:users(id)"`
	Body      string    `db:"body"`
	CreatedAt time.Time `db:"created_at" norm:"not_null,default:now()"`
}

// MoneyTest -> money_tests (decimal type override)
type MoneyTest struct {
	ID        int64     `db:"id" norm:"primary_key,auto_increment"`
	Amount    float64   `db:"amount" norm:"type:decimal(20,8)"`
	CreatedAt time.Time `db:"created_at" norm:"not_null,default:now()"`
}

// Account -> accounts (composite unique on tenant_id+slug)
type Account struct {
	ID       int64  `db:"id" norm:"primary_key,auto_increment"`
	TenantID int64  `db:"tenant_id" norm:"not_null,unique:tenant_slug"`
	Slug     string `db:"slug" norm:"not_null,unique:tenant_slug"`
}

// PartialIdx -> partial_idxs (partial index on email when not deleted)
type PartialIdx struct {
	ID        int64      `db:"id" norm:"primary_key,auto_increment"`
	Email     string     `db:"email" norm:"index,index_where:(deleted_at IS NULL)"`
	DeletedAt *time.Time `db:"deleted_at"`
}

// IgnoreField -> ignore_fields (ignored column not created)
type IgnoreField struct {
	ID   int64  `db:"id" norm:"primary_key,auto_increment"`
	Name string `db:"name"`
	Temp string `db:"temp" norm:"-"`
}

// CascadeParent/CascadeChild for FK actions
type CascadeParent struct {
	ID   int64  `db:"id" norm:"primary_key,auto_increment"`
	Name string `db:"name"`
}
type CascadeChild struct {
	ID       int64 `db:"id" norm:"primary_key,auto_increment"`
	ParentID int64 `db:"parent_id" norm:"not_null,fk:cascade_parents(id),on_delete:cascade,fk_name:fk_child_parent"`
}

var kn *kintsnorm.KintsNorm

func TestMain(m *testing.M) {
	// context reserved for future use (timeouts per connection if needed)
	// ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	// defer cancel()

	host := getenvDefault("PGHOST", "127.0.0.1")
	port := getenvDefault("PGPORT", "5432")
	user := getenvDefault("PGUSER", "postgres")
	pass := getenvDefault("PGPASSWORD", "postgres")
	db := getenvDefault("PGDATABASE", "postgres")

	if err := waitTCP(host, port, 30*time.Second); err != nil {
		fmt.Println("postgres not reachable:", err)
		os.Exit(1)
	}

	dsn := fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=disable", host, port, db, user, pass)
	var err error
	kn, err = kintsnorm.NewWithConnString(dsn)
	if err != nil {
		fmt.Println("failed to connect pg:", err)
		os.Exit(1)
	}

	code := m.Run()
	_ = kn.Close()
	os.Exit(code)
}

func TestHealthAndMigrate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := kn.Health(ctx); err != nil {
		t.Fatalf("health failed: %v", err)
	}

	// Plan and log statements for debugging then migrate
	mg := migration.NewMigrator(kn.Pool())
	plan, perr := mg.Plan(ctx, &User{}, &Profile{}, &MoneyTest{}, &Account{}, &PartialIdx{}, &IgnoreField{}, &CascadeParent{}, &CascadeChild{})
	if perr != nil {
		t.Fatalf("plan failed: %v", perr)
	}
	for _, s := range plan.Statements {
		t.Logf("PLAN STMT: %s", s)
	}
	// Manually apply plan statements to detect any syntax issues precisely
	for _, s := range plan.Statements {
		if _, err := kn.Pool().Exec(ctx, s); err != nil {
			t.Fatalf("failed executing plan stmt: %s; err=%v", s, err)
		}
	}
	// Run automigrate to record schema_migrations entry
	if err := kn.AutoMigrate(&User{}, &Profile{}, &MoneyTest{}, &Account{}, &PartialIdx{}, &IgnoreField{}, &CascadeParent{}, &CascadeChild{}); err != nil {
		t.Fatalf("automigrate failed: %v", err)
	}

	// verify table exists
	var regclass *string
	if err := kn.Pool().QueryRow(ctx, "select to_regclass('public.users')").Scan(&regclass); err != nil {
		t.Fatalf("regclass check failed: %v", err)
	}
	if regclass == nil || *regclass != "users" {
		t.Fatalf("users table not found, got: %v", regclass)
	}

	// verify profiles exists
	var regclassP *string
	if err := kn.Pool().QueryRow(ctx, "select to_regclass('public.profiles')").Scan(&regclassP); err != nil {
		t.Fatalf("regclass profile check failed: %v", err)
	}
	if regclassP == nil || *regclassP != "profiles" {
		t.Fatalf("profiles table not found, got: %v", regclassP)
	}

	// verify indexes from tags (users.email unique index)
	rows, err := kn.Pool().Query(ctx, "select indexname from pg_indexes where schemaname='public' and tablename='users'")
	if err != nil {
		t.Fatalf("pg_indexes: %v", err)
	}
	defer rows.Close()
	hasEmailIdx := false
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan idx: %v", err)
		}
		if name == "idx_users_email" {
			hasEmailIdx = true
		}
	}
	if !hasEmailIdx {
		t.Fatalf("expected idx_users_email to exist")
	}

	// verify MoneyTest.amount is DECIMAL(20,8)
	var prec, scale int
	if err := kn.Pool().QueryRow(ctx, `SELECT COALESCE(numeric_precision,0), COALESCE(numeric_scale,0) FROM information_schema.columns WHERE table_schema='public' AND table_name='money_tests' AND column_name='amount'`).Scan(&prec, &scale); err != nil {
		t.Fatalf("inspect decimal: %v", err)
	}
	if prec != 20 || scale != 8 {
		t.Fatalf("expected decimal(20,8), got (%d,%d)", prec, scale)
	}

	// composite unique on accounts(tenant_id, slug)
	var hasUq bool
	idxRows, err := kn.Pool().Query(ctx, `SELECT indexname, indexdef FROM pg_indexes WHERE schemaname='public' AND tablename='accounts'`)
	if err != nil {
		t.Fatalf("pg_indexes accounts: %v", err)
	}
	defer idxRows.Close()
	for idxRows.Next() {
		var name, def string
		if err := idxRows.Scan(&name, &def); err != nil {
			t.Fatalf("scan accounts idx: %v", err)
		}
		if strings.Contains(def, "UNIQUE") && strings.Contains(def, "tenant_id") && strings.Contains(def, "slug") {
			hasUq = true
		}
	}
	if !hasUq {
		t.Fatalf("expected composite unique on accounts(tenant_id, slug)")
	}

	// partial index exists on partial_idxs(email)
	var hasPartial bool
	prow, err := kn.Pool().Query(ctx, `SELECT indexdef FROM pg_indexes WHERE schemaname='public' AND tablename='partial_idxs'`)
	if err != nil {
		t.Fatalf("pg_indexes partial: %v", err)
	}
	defer prow.Close()
	for prow.Next() {
		var def string
		if err := prow.Scan(&def); err != nil {
			t.Fatalf("scan partial idx: %v", err)
		}
		if strings.Contains(def, "WHERE (deleted_at IS NULL)") {
			hasPartial = true
		}
	}
	if !hasPartial {
		t.Fatalf("expected partial index with WHERE (deleted_at IS NULL)")
	}

	// ignored field 'temp' not created
	var c int
	if err := kn.Pool().QueryRow(ctx, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='ignore_fields' AND column_name='temp'`).Scan(&c); err != nil {
		t.Fatalf("inspect ignore_fields: %v", err)
	}
	if c != 0 {
		t.Fatalf("expected temp column to be ignored, got count=%d", c)
	}

	// verify schema_migrations row written and idempotent
	var cnt int
	if err := kn.Pool().QueryRow(ctx, "select count(*) from schema_migrations").Scan(&cnt); err != nil {
		t.Fatalf("schema_migrations count: %v", err)
	}
	if cnt < 1 {
		t.Fatalf("expected at least one migration record, got %d", cnt)
	}
	if err := kn.AutoMigrate(&User{}, &Profile{}); err != nil {
		t.Fatalf("automigrate rerun: %v", err)
	}
	var cnt2 int
	if err := kn.Pool().QueryRow(ctx, "select count(*) from schema_migrations").Scan(&cnt2); err != nil {
		t.Fatalf("schema_migrations count2: %v", err)
	}
	if cnt2 < cnt {
		t.Fatalf("migration count decreased unexpectedly")
	}
}

func TestRepositoryCRUDAndSoftDelete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// cleanup
	_, _ = kn.Pool().Exec(ctx, "TRUNCATE users RESTART IDENTITY CASCADE")
	_, _ = kn.Pool().Exec(ctx, "TRUNCATE accounts RESTART IDENTITY CASCADE")

	repo := kintsnorm.NewRepository[User](kn)
	u := &User{Email: "alice@example.com", Username: "alice", Password: "secret", IsActive: true}
	if err := repo.Create(ctx, u); err != nil {
		t.Fatalf("create failed: %v", err)
	}

	// fetch back via FindOne to get ID
	got, err := repo.FindOne(ctx, kintsnorm.Condition{Expr: "email = ?", Args: []any{"alice@example.com"}})
	if err != nil {
		t.Fatalf("findone failed: %v", err)
	}
	if got.Username != "alice" {
		t.Fatalf("unexpected user: %+v", got)
	}
	if got.DeletedAt != nil {
		t.Fatalf("expected DeletedAt to be nil before soft delete")
	}
	if got.CreatedAt.IsZero() {
		t.Fatalf("expected created_at to be set by DB default")
	}

	// Update
	beforeUpdate := got.UpdatedAt
	got.Password = "newpass"
	if err := repo.Update(ctx, got); err != nil {
		t.Fatalf("update failed: %v", err)
	}
	gAfter, err := repo.FindOne(ctx, kintsnorm.Condition{Expr: "id = ?", Args: []any{got.ID}})
	if err != nil {
		t.Fatalf("re-fetch after update: %v", err)
	}
	if !gAfter.UpdatedAt.After(beforeUpdate) {
		t.Fatalf("expected updated_at to move forward: before=%v after=%v", beforeUpdate, gAfter.UpdatedAt)
	}

	// UpdatePartial
	b2 := gAfter.UpdatedAt
	if err := repo.UpdatePartial(ctx, got.ID, map[string]any{"username": "alice2"}); err != nil {
		t.Fatalf("update partial failed: %v", err)
	}
	g2, err := repo.FindOne(ctx, kintsnorm.Condition{Expr: "id = ?", Args: []any{got.ID}})
	if err != nil || g2.Username != "alice2" {
		t.Fatalf("partial not applied: %+v err=%v", g2, err)
	}
	if !g2.UpdatedAt.After(b2) {
		t.Fatalf("expected updated_at to move after partial update: before=%v after=%v", b2, g2.UpdatedAt)
	}

	// Count/Exists
	c, err := repo.Count(ctx, kintsnorm.Condition{Expr: "is_active = ?", Args: []any{true}})
	if err != nil || c < 1 {
		t.Fatalf("count failed: %v %d", err, c)
	}
	ex, err := repo.Exists(ctx, kintsnorm.Condition{Expr: "email = ?", Args: []any{"alice@example.com"}})
	if err != nil || !ex {
		t.Fatalf("exists failed: %v %v", err, ex)
	}

	// Soft delete hides rows from default queries
	// first create user name with soft delete
	soft := &User{Email: "soft@example.com", Username: "soft", Password: "x", IsActive: true}
	if err := repo.Create(ctx, soft); err != nil {
		t.Fatalf("create soft: %v", err)
	}
	// fetch created row to get generated ID (Create does not RETURNING id)
	softRow, err := repo.FindOne(ctx, kintsnorm.Condition{Expr: "email = ?", Args: []any{"soft@example.com"}})
	if err != nil {
		t.Fatalf("find soft: %v", err)
	}
	if err := repo.SoftDelete(ctx, softRow.ID); err != nil {
		t.Fatalf("soft delete: %v", err)
	}

	// DeletedAt should be set; verify
	var deletedAt *time.Time
	if err := kn.Pool().QueryRow(ctx, "select deleted_at from users where id = $1", softRow.ID).Scan(&deletedAt); err != nil {
		t.Fatalf("query deleted_at: %v", err)
	}
	if deletedAt == nil {
		t.Fatalf("deleted_at not set")
	}

	// Default Find should not return soft-deleted rows
	if ex, err := repo.Exists(ctx, kintsnorm.Condition{Expr: "id = ?", Args: []any{softRow.ID}}); err != nil {
		t.Fatalf("exists error: %v", err)
	} else if ex {
		t.Fatalf("soft-deleted row should be hidden in Exists/Count")
	}
	if _, err := repo.FindOne(ctx, kintsnorm.Condition{Expr: "id = ?", Args: []any{softRow.ID}}); err == nil {
		t.Fatalf("soft-deleted row should not be returned by FindOne")
	}

	// Scope checks: OnlyTrashed should find it, WithTrashed should allow both
	if _, err := repo.OnlyTrashed().FindOne(ctx, kintsnorm.Condition{Expr: "id = ?", Args: []any{softRow.ID}}); err != nil {
		t.Fatalf("only trashed should find deleted row: %v", err)
	}
	if _, err := repo.WithTrashed().FindOne(ctx, kintsnorm.Condition{Expr: "id = ?", Args: []any{softRow.ID}}); err != nil {
		t.Fatalf("with trashed should find deleted row: %v", err)
	}

	// Create additional users and soft delete all
	_ = repo.Create(ctx, &User{Email: "x1@example.com", Username: "x1", Password: "x"})
	_ = repo.Create(ctx, &User{Email: "x2@example.com", Username: "x2", Password: "x"})
	if n, err := repo.SoftDeleteAll(ctx); err != nil || n < 2 {
		t.Fatalf("soft delete all failed or affected too few rows: n=%d err=%v", n, err)
	}
	// Ensure no non-deleted rows remain
	if c, err := repo.Count(ctx); err != nil {
		t.Fatalf("count after soft delete all: %v", err)
	} else if c != 0 {
		t.Fatalf("expected 0 active rows, got %d", c)
	}

	// Unique violation on email
	if err := repo.Create(ctx, &User{Email: "alice@example.com", Username: "alice3", Password: "x"}); err == nil {
		t.Fatalf("expected unique violation on email")
	}

	// Composite unique on accounts: (tenant_id, slug)
	if err := kn.AutoMigrate(&Account{}); err != nil {
		t.Fatalf("migrate account: %v", err)
	}
	if _, err := kn.Pool().Exec(ctx, `INSERT INTO accounts(tenant_id, slug) VALUES ($1,$2)`, 1, "a"); err != nil {
		t.Fatalf("seed account: %v", err)
	}
	if _, err := kn.Pool().Exec(ctx, `INSERT INTO accounts(tenant_id, slug) VALUES ($1,$2)`, 1, "a"); err == nil {
		t.Fatalf("expected composite unique violation on accounts(tenant_id, slug)")
	}

	// Hard delete
	if err := repo.Delete(ctx, got.ID); err != nil {
		t.Fatalf("delete hard: %v", err)
	}
	if ex, _ := repo.Exists(ctx, kintsnorm.Condition{Expr: "id = ?", Args: []any{got.ID}}); ex {
		t.Fatalf("hard delete not effective")
	}
}

func TestTransactionCommitRollback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// rollback
	_ = kn.Tx().WithTransaction(ctx, func(tx kintsnorm.Transaction) error {
		r := kintsnorm.NewRepositoryWithExecutor[User](kn, tx.Exec())
		if err := r.Create(ctx, &User{Email: "bob@example.com", Username: "bob", Password: "pw"}); err != nil {
			t.Fatalf("create in tx failed: %v", err)
		}
		return fmt.Errorf("force rollback")
	})

	repo := kintsnorm.NewRepository[User](kn)
	if ex, err := repo.Exists(ctx, kintsnorm.Condition{Expr: "email = ?", Args: []any{"bob@example.com"}}); err != nil || ex {
		t.Fatalf("rollback failed, exists=%v err=%v", ex, err)
	}

	// commit
	if err := kn.Tx().WithTransaction(ctx, func(tx kintsnorm.Transaction) error {
		r := kintsnorm.NewRepositoryWithExecutor[User](kn, tx.Exec())
		return r.Create(ctx, &User{Email: "carol@example.com", Username: "carol", Password: "pw"})
	}); err != nil {
		t.Fatalf("commit tx failed: %v", err)
	}
	if ex, err := repo.Exists(ctx, kintsnorm.Condition{Expr: "email = ?", Args: []any{"carol@example.com"}}); err != nil || !ex {
		t.Fatalf("commit not visible, exists=%v err=%v", ex, err)
	}
}

func TestQueryBuilderInjectionSafety(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// attempt injection through parameter should not execute extra statements
	var rows []User
	err := kn.Query().Table("users").Where("username = ?", "x'; DROP TABLE users;--").Find(ctx, &rows)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	// ensure users still exists
	var regclass *string
	if err := kn.Pool().QueryRow(ctx, "select to_regclass('public.users')").Scan(&regclass); err != nil {
		t.Fatalf("regclass check failed: %v", err)
	}
	if regclass == nil || *regclass != "users" {
		t.Fatalf("users table missing after injection attempt")
	}
}

func TestAutoMigrateIdempotent(t *testing.T) {
	if err := kn.AutoMigrate(&User{}, &Profile{}); err != nil {
		t.Fatalf("automigrate #1: %v", err)
	}
	if err := kn.AutoMigrate(&User{}, &Profile{}); err != nil {
		t.Fatalf("automigrate #2 should be idempotent: %v", err)
	}
}

func TestMultipleWhereArgsOrdering(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _ = kn.Pool().Exec(ctx, "TRUNCATE users RESTART IDENTITY CASCADE")
	repo := kintsnorm.NewRepository[User](kn)
	_ = repo.Create(ctx, &User{Email: "mw@example.com", Username: "mw", Password: "x", IsActive: true})
	_ = repo.Create(ctx, &User{Email: "mw2@example.com", Username: "mw2", Password: "x", IsActive: false})
	var out []User
	if err := kn.Query().Table("users").Where("email = ?", "mw@example.com").Where("is_active = ?", true).Find(ctx, &out); err != nil {
		t.Fatalf("multi-where failed: %v", err)
	}
	if len(out) != 1 || out[0].Email != "mw@example.com" {
		t.Fatalf("unexpected result for multi-where: %+v", out)
	}
}

func TestRawExecDDLAndInsertSelect(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := kn.Query().Raw("CREATE TABLE IF NOT EXISTS calc_test (a int, b int)").Exec(ctx); err != nil {
		t.Fatalf("create table: %v", err)
	}
	// clean
	_, _ = kn.Pool().Exec(ctx, "TRUNCATE calc_test")
	if err := kn.Query().Raw("INSERT INTO calc_test(a,b) VALUES(?,?)", 7, 5).Exec(ctx); err != nil {
		t.Fatalf("insert: %v", err)
	}
	var res []map[string]any
	if err := kn.Query().Raw("SELECT a + b AS s FROM calc_test").Find(ctx, &res); err != nil {
		t.Fatalf("select: %v", err)
	}
	if len(res) != 1 || fmt.Sprint(res[0]["s"]) != "12" {
		t.Fatalf("unexpected sum: %+v", res)
	}
}

func TestQueryBuilderJoinsPaginationRawAndTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// setup data
	_, _ = kn.Pool().Exec(ctx, "TRUNCATE users RESTART IDENTITY CASCADE")
	_, _ = kn.Pool().Exec(ctx, "TRUNCATE profiles RESTART IDENTITY CASCADE")

	repoU := kintsnorm.NewRepository[User](kn)
	// batch create
	batch := []*User{}
	for i := 0; i < 15; i++ {
		batch = append(batch, &User{Email: fmt.Sprintf("u%02d@example.com", i), Username: fmt.Sprintf("u%02d", i), Password: "pw", IsActive: i%2 == 0})
	}
	if err := repoU.CreateBatch(ctx, batch); err != nil {
		t.Fatalf("batch create: %v", err)
	}

	// pagination: get users 6..15
	var users []User
	if err := kn.Query().Table("users").OrderBy("id ASC").Limit(10).Offset(5).Find(ctx, &users); err != nil {
		t.Fatalf("pagination find: %v", err)
	}
	if len(users) != 10 {
		t.Fatalf("expected 10, got %d", len(users))
	}

	// create a profile for user id 1
	if _, err := kn.Pool().Exec(ctx, "INSERT INTO profiles(user_id, bio) VALUES ($1,$2)", 1, "bio-1"); err != nil {
		t.Fatalf("insert profile: %v", err)
	}

	// join query (INNER)
	var joined []map[string]any
	if err := kn.Query().Table("users u").Join("profiles p", "u.id = p.user_id").Select("u.id", "p.bio").Find(ctx, &joined); err != nil {
		t.Fatalf("join find: %v", err)
	}
	if len(joined) != 1 || fmt.Sprint(joined[0]["bio"]) != "bio-1" {
		t.Fatalf("unexpected join result: %+v", joined)
	}

	// explicit InnerJoin alias
	joined = nil
	if err := kn.Query().Table("users u").InnerJoin("profiles p", "u.id = p.user_id").Select("u.id", "p.bio").Find(ctx, &joined); err != nil {
		t.Fatalf("inner join find: %v", err)
	}
	if len(joined) != 1 {
		t.Fatalf("expected 1 row for inner join, got %d", len(joined))
	}

	// LEFT JOIN returns all users with possible NULL bio
	var leftRows []map[string]any
	if err := kn.Query().Table("users u").LeftJoin("profiles p", "u.id = p.user_id").Select("u.id", "p.bio").Find(ctx, &leftRows); err != nil {
		t.Fatalf("left join find: %v", err)
	}
	if len(leftRows) != 15 {
		t.Fatalf("expected 15 rows for left join (all users), got %d", len(leftRows))
	}

	// RIGHT JOIN returns all profiles (only one exists)
	var rightRows []map[string]any
	if err := kn.Query().Table("users u").RightJoin("profiles p", "u.id = p.user_id").Select("u.id", "p.bio").Find(ctx, &rightRows); err != nil {
		t.Fatalf("right join find: %v", err)
	}
	if len(rightRows) != 1 {
		t.Fatalf("expected 1 row for right join (all profiles), got %d", len(rightRows))
	}

	// FULL JOIN returns all users plus unmatched profiles (none extra here) => 15
	var fullRows []map[string]any
	if err := kn.Query().Table("users u").FullJoin("profiles p", "u.id = p.user_id").Select("u.id", "p.bio").Find(ctx, &fullRows); err != nil {
		t.Fatalf("full join find: %v", err)
	}
	if len(fullRows) != 15 {
		t.Fatalf("expected 15 rows for full join, got %d", len(fullRows))
	}

	// CROSS JOIN with a constant subquery keeps cardinality (users x 1) => 15
	var crossRows []map[string]any
	if err := kn.Query().Table("users u").CrossJoin("(SELECT 1) x").Select("u.id").Find(ctx, &crossRows); err != nil {
		t.Fatalf("cross join find: %v", err)
	}
	if len(crossRows) != 15 {
		t.Fatalf("expected 15 rows for cross join with const, got %d", len(crossRows))
	}

	// Raw with placeholders and multiple clauses
	var calc []map[string]any
	if err := kn.Query().Raw("select (?::int + ?::int) as sum where ?::int = ?::int", 2, 3, 1, 1).Find(ctx, &calc); err != nil {
		t.Fatalf("raw find: %v", err)
	}
	if len(calc) != 1 || fmt.Sprint(calc[0]["sum"]) != "5" {
		t.Fatalf("raw result unexpected: %+v", calc)
	}

	// context timeout via pg_sleep
	tctx, tcancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer tcancel()
	var sleeper []map[string]any
	if err := kn.Query().Raw("select pg_sleep(0.2)").Find(tctx, &sleeper); err == nil {
		t.Fatalf("expected context timeout on pg_sleep")
	}
}

func TestQueryBuilderFirstLastAndDelete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _ = kn.Pool().Exec(ctx, "TRUNCATE users RESTART IDENTITY CASCADE")
	// seed
	_, _ = kn.Pool().Exec(ctx, "INSERT INTO users(email, username, password) VALUES ($1,$2,$3),($4,$5,$6),($7,$8,$9)",
		"f1@example.com", "f1", "x",
		"f2@example.com", "f2", "x",
		"f3@example.com", "f3", "x",
	)

	// First
	var u User
	if err := kn.Query().Table("users").OrderBy("id ASC").First(ctx, &u); err != nil {
		t.Fatalf("first: %v", err)
	}
	if u.Username != "f1" {
		t.Fatalf("unexpected first: %+v", u)
	}

	// Last requires OrderBy
	var ul User
	if err := kn.Query().Table("users").OrderBy("id ASC").Last(ctx, &ul); err != nil {
		t.Fatalf("last: %v", err)
	}
	if ul.Username != "f3" {
		t.Fatalf("unexpected last: %+v", ul)
	}

	// Last without OrderBy should return validation error
	var dummy User
	if err := kn.Query().Table("users").Last(ctx, &dummy); err == nil {
		t.Fatalf("expected error when Last called without OrderBy")
	}

	// Delete using builder (soft delete by default)
	aff, err := kn.Query().Table("users").Where("username = ?", "f2").Delete(ctx)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if aff != 1 {
		t.Fatalf("expected 1 row deleted, got %d", aff)
	}

	// Ensure f2 is hidden from active queries (deleted_at IS NULL)
	var rows []User
	if err := kn.Query().Table("users").Where("username = ?", "f2").Where("deleted_at IS NULL").Find(ctx, &rows); err != nil {
		t.Fatalf("find: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected no rows for f2, got %d", len(rows))
	}
}

func TestModelChainSelectFirstLastAndCRUD(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _ = kn.Pool().Exec(ctx, "TRUNCATE users RESTART IDENTITY CASCADE")

	// seed 3 users
	_, _ = kn.Pool().Exec(ctx, "INSERT INTO users(email, username, password) VALUES ($1,$2,$3),($4,$5,$6),($7,$8,$9)",
		"m1@example.com", "m1", "x",
		"m2@example.com", "m2", "x",
		"m3@example.com", "m3", "x",
	)

	// First via KintsNorm.Model
	var first User
	if err := kn.Model(&User{}).OrderBy("id ASC").First(ctx, &first); err != nil {
		t.Fatalf("model first: %v", err)
	}
	if first.Username != "m1" {
		t.Fatalf("unexpected first via Model: %+v", first)
	}

	// Last via Query().Model
	var last User
	if err := kn.Query().Model(User{}).OrderBy("id ASC").Last(ctx, &last); err != nil {
		t.Fatalf("model last: %v", err)
	}
	if last.Username != "m3" {
		t.Fatalf("unexpected last via Model: %+v", last)
	}

	// Find with filter via Model
	var out []User
	if err := kn.Model(&User{}).Where("email = ?", "m2@example.com").Find(ctx, &out); err != nil {
		t.Fatalf("model find: %v", err)
	}
	if len(out) != 1 || out[0].Username != "m2" {
		t.Fatalf("unexpected model find: %+v", out)
	}

	// Insert using Model + Returning
	var ret []map[string]any
	aff, err := kn.Model(User{}).Insert("email", "username", "password").Values("mr@example.com", "mr", "x").Returning("id", "email").ExecInsert(ctx, &ret)
	if err != nil || aff != 1 {
		t.Fatalf("model insert returning: aff=%d err=%v", aff, err)
	}
	if len(ret) != 1 || ret[0]["email"] != "mr@example.com" {
		t.Fatalf("unexpected model insert ret: %+v", ret)
	}

	// Update using Model + Returning
	ret = nil
	aff2, err := kn.Query().Model(&User{}).Set("password = ?", "y").Where("email = ?", "mr@example.com").Returning("id", "password").ExecUpdate(ctx, &ret)
	if err != nil || aff2 != 1 {
		t.Fatalf("model update returning: aff=%d err=%v", aff2, err)
	}
	if len(ret) != 1 {
		t.Fatalf("model update returning rows: %+v", ret)
	}

	// Soft delete via Model
	delAff, err := kn.Model(User{}).Where("username = ?", "m2").Delete(ctx)
	if err != nil || delAff != 1 {
		t.Fatalf("model soft delete: aff=%d err=%v", delAff, err)
	}
	// verify deleted_at set
	var deletedAt *time.Time
	if err := kn.Pool().QueryRow(ctx, "select deleted_at from users where username=$1", "m2").Scan(&deletedAt); err != nil {
		t.Fatalf("check deleted_at after model delete: %v", err)
	}
	if deletedAt == nil {
		t.Fatalf("expected deleted_at set after model soft delete")
	}
}

func TestInsertReturningUpsertAndUpdateReturning(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _ = kn.Pool().Exec(ctx, "TRUNCATE users RESTART IDENTITY CASCADE")

	// Insert with RETURNING
	var ret []map[string]any
	aff, err := kn.Query().Table("users").Insert("email", "username", "password").Values("ret1@example.com", "ret1", "x").Returning("id", "email").ExecInsert(ctx, &ret)
	if err != nil || aff != 1 {
		t.Fatalf("insert return: aff=%d err=%v", aff, err)
	}
	if len(ret) != 1 || ret[0]["email"] != "ret1@example.com" {
		t.Fatalf("unexpected returning: %+v", ret)
	}

	// Upsert (ON CONFLICT DO UPDATE)
	ret = nil
	aff, err = kn.Query().Table("users").Insert("email", "username", "password").Values("ret1@example.com", "ret1b", "x").OnConflict("email").DoUpdateSet("username = ?", "ret1-upd").Returning("id", "username").ExecInsert(ctx, &ret)
	if err != nil || aff != 1 {
		t.Fatalf("upsert: aff=%d err=%v", aff, err)
	}
	if len(ret) != 1 || ret[0]["username"] != "ret1-upd" {
		t.Fatalf("upsert returning unexpected: %+v", ret)
	}
	// verify only one row with email exists and value updated
	var cnt int
	if err := kn.Pool().QueryRow(ctx, "select count(*) from users where email=$1", "ret1@example.com").Scan(&cnt); err != nil {
		t.Fatalf("count upsert row: %v", err)
	}
	if cnt != 1 {
		t.Fatalf("expected 1 row for upserted email, got %d", cnt)
	}

	// Update with RETURNING
	ret = nil
	aff2, err := kn.Query().Table("users").Set("password = ?", "y").Where("email = ?", "ret1@example.com").Returning("id", "password").ExecUpdate(ctx, &ret)
	if err != nil || aff2 != 1 {
		t.Fatalf("update returning: aff=%d err=%v", aff2, err)
	}
	if len(ret) != 1 {
		t.Fatalf("update returning rows: %+v", ret)
	}
}

func TestRepositoryUpsert(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _ = kn.Pool().Exec(ctx, "TRUNCATE users RESTART IDENTITY CASCADE")

	repo := kintsnorm.NewRepository[User](kn)
	u := &User{Email: "rup@example.com", Username: "rup1", Password: "x"}
	if err := repo.Upsert(ctx, u, []string{"email"}, []string{"username", "password"}); err != nil {
		t.Fatalf("upsert insert: %v", err)
	}
	got, err := repo.FindOne(ctx, kintsnorm.Condition{Expr: "email = ?", Args: []any{"rup@example.com"}})
	if err != nil || got.Username != "rup1" {
		t.Fatalf("after first upsert unexpected: %+v err=%v", got, err)
	}
	// change username and run upsert again -> should update existing row
	u.Username = "rup2"
	if err := repo.Upsert(ctx, u, []string{"email"}, []string{"username"}); err != nil {
		t.Fatalf("upsert update: %v", err)
	}
	got2, err := repo.FindOne(ctx, kintsnorm.Condition{Expr: "email = ?", Args: []any{"rup@example.com"}})
	if err != nil || got2.Username != "rup2" {
		t.Fatalf("after second upsert unexpected: %+v err=%v", got2, err)
	}
}

func TestTxQueryBuilderAndReadPoolFallback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _ = kn.Pool().Exec(ctx, "TRUNCATE users RESTART IDENTITY CASCADE")
	// use Tx().Query
	if err := kn.Tx().WithTransaction(ctx, func(tx kintsnorm.Transaction) error {
		var ret []map[string]any
		_, err := tx.Query().Table("users").Insert("email", "username", "password").Values("tq@example.com", "tq", "x").Returning("id").ExecInsert(ctx, &ret)
		if err != nil {
			return err
		}
		if len(ret) != 1 {
			return fmt.Errorf("expected 1 returning row")
		}
		return nil
	}); err != nil {
		t.Fatalf("tx query builder: %v", err)
	}

	// ReadPool should fallback to Pool when read-only not configured (no panic)
	if kn.ReadPool() == nil {
		t.Fatalf("read pool fallback missing")
	}
}

func TestKeysetPaginationAfterBefore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _ = kn.Pool().Exec(ctx, "TRUNCATE users RESTART IDENTITY CASCADE")
	// seed 10
	for i := 0; i < 10; i++ {
		_, _ = kn.Pool().Exec(ctx, "INSERT INTO users(email, username, password) VALUES ($1,$2,$3)", fmt.Sprintf("k%02d@example.com", i), fmt.Sprintf("k%02d", i), "x")
	}
	// page forward after id=3
	var p1 []User
	if err := kn.Query().Table("users").OrderBy("id ASC").After("id", 3).Limit(3).Find(ctx, &p1); err != nil {
		t.Fatalf("keyset after: %v", err)
	}
	if len(p1) != 3 || p1[0].ID != 4 || p1[2].ID != 6 {
		t.Fatalf("unexpected keyset after page: %+v", p1)
	}
	// page back before id=5 with DESC ordering returns rows with id > 5 (earlier in ordered list)
	var p2 []User
	if err := kn.Query().Table("users").OrderBy("id DESC").Before("id", 5).Limit(2).Find(ctx, &p2); err != nil {
		t.Fatalf("keyset before: %v", err)
	}
	if len(p2) != 2 || p2[0].ID != 10 || p2[1].ID != 9 {
		t.Fatalf("unexpected keyset before page: %+v", p2)
	}
}

func TestRepositoryFindPageWithOrderingAndScopes(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _ = kn.Pool().Exec(ctx, "TRUNCATE users RESTART IDENTITY CASCADE")
	repo := kintsnorm.NewRepository[User](kn)
	// seed 12 users
	for i := 0; i < 12; i++ {
		_ = repo.Create(ctx, &User{Email: fmt.Sprintf("p%02d@example.com", i), Username: fmt.Sprintf("p%02d", i), Password: "x"})
	}
	// soft delete a few
	_ = repo.SoftDelete(ctx, 3)
	_ = repo.SoftDelete(ctx, 4)
	_ = repo.SoftDelete(ctx, 7)

	// page 1 (limit 5, offset 0) active only
	pr := kintsnorm.PageRequest{Limit: 5, Offset: 0, OrderBy: "id ASC"}
	page, err := repo.FindPage(ctx, pr)
	if err != nil {
		t.Fatalf("find page: %v", err)
	}
	if page.Total != 9 {
		t.Fatalf("expected total 9 active records, got %d", page.Total)
	}
	if len(page.Items) != 5 {
		t.Fatalf("expected 5 items, got %d", len(page.Items))
	}
	if page.Items[0].ID != 1 || page.Items[1].ID != 2 {
		t.Fatalf("unexpected first items: %+v", page.Items[:2])
	}

	// with trashed: count includes deleted
	pageAll, err := repo.WithTrashed().FindPage(ctx, pr)
	if err != nil {
		t.Fatalf("find page with trashed: %v", err)
	}
	if pageAll.Total != 12 {
		t.Fatalf("expected total 12 with trashed, got %d", pageAll.Total)
	}

	// only trashed: total 3
	pageDel, err := repo.OnlyTrashed().FindPage(ctx, pr)
	if err != nil {
		t.Fatalf("find page only trashed: %v", err)
	}
	if pageDel.Total != 3 {
		t.Fatalf("expected total 3 only trashed, got %d", pageDel.Total)
	}
}

func TestCommonScopesDateRangeAndOnDate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _ = kn.Pool().Exec(ctx, "TRUNCATE users RESTART IDENTITY CASCADE")
	repo := kintsnorm.NewRepository[User](kn)

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	// create users with spaced created_at using raw insert to control timestamps
	for i := 0; i < 5; i++ {
		ts := base.Add(time.Duration(i) * 24 * time.Hour)
		// id and created_at explicit
		if _, err := kn.Pool().Exec(ctx, `INSERT INTO users(id, email, username, password, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$5)`, int64(i+1), fmt.Sprintf("d%02d@example.com", i), fmt.Sprintf("d%02d", i), "x", ts); err != nil {
			t.Fatalf("seed dated user %d: %v", i, err)
		}
	}

	// DateRange: include day 2..4
	cond := kintsnorm.DateRange("created_at", base.Add(24*time.Hour), base.Add(4*24*time.Hour))
	items, err := repo.Find(ctx, cond)
	if err != nil {
		t.Fatalf("date range find: %v", err)
	}
	if len(items) != 4 { // days 1,2,3,4 (since inclusive)
		t.Fatalf("expected 4 items in range, got %d", len(items))
	}

	// OnDate: exactly third day
	cond2 := kintsnorm.OnDate("created_at", base.Add(2*24*time.Hour))
	items2, err := repo.Find(ctx, cond2)
	if err != nil {
		t.Fatalf("on date find: %v", err)
	}
	if len(items2) != 1 || items2[0].Username != "d02" {
		t.Fatalf("expected one item for specific day, got %+v", items2)
	}
}

func TestPaginationOrderingAndOffsetEdge(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _ = kn.Pool().Exec(ctx, "TRUNCATE users RESTART IDENTITY CASCADE")
	repo := kintsnorm.NewRepository[User](kn)
	for i := 0; i < 6; i++ {
		_ = repo.Create(ctx, &User{Email: fmt.Sprintf("o%02d@example.com", i), Username: fmt.Sprintf("o%02d", i), Password: "x"})
	}
	// DESC order page
	pr := kintsnorm.PageRequest{Limit: 2, Offset: 0, OrderBy: "id DESC"}
	p, err := repo.FindPage(ctx, pr)
	if err != nil {
		t.Fatalf("find page desc: %v", err)
	}
	if len(p.Items) != 2 || p.Items[0].ID != 6 || p.Items[1].ID != 5 {
		t.Fatalf("unexpected desc page: %+v", p.Items)
	}
	// Offset beyond total
	pr2 := kintsnorm.PageRequest{Limit: 5, Offset: 100, OrderBy: "id ASC"}
	p2, err := repo.FindPage(ctx, pr2)
	if err != nil {
		t.Fatalf("find page beyond: %v", err)
	}
	if p2.Total != 6 || len(p2.Items) != 0 {
		t.Fatalf("unexpected beyond page: total=%d items=%d", p2.Total, len(p2.Items))
	}
}

func TestRestoreAndPurgeTrashed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _ = kn.Pool().Exec(ctx, "TRUNCATE users RESTART IDENTITY CASCADE")
	repo := kintsnorm.NewRepository[User](kn)
	for i := 0; i < 4; i++ {
		_ = repo.Create(ctx, &User{Email: fmt.Sprintf("r%02d@example.com", i), Username: fmt.Sprintf("r%02d", i), Password: "x"})
	}
	_ = repo.SoftDelete(ctx, 2)
	_ = repo.SoftDelete(ctx, 3)
	// only trashed total 2
	pDel, err := repo.OnlyTrashed().FindPage(ctx, kintsnorm.PageRequest{Limit: 10})
	if err != nil || pDel.Total != 2 {
		t.Fatalf("only trashed total expected 2: %+v err=%v", pDel, err)
	}
	// restore one
	if err := repo.Restore(ctx, 2); err != nil {
		t.Fatalf("restore: %v", err)
	}
	pDel2, _ := repo.OnlyTrashed().FindPage(ctx, kintsnorm.PageRequest{Limit: 10})
	if pDel2.Total != 1 {
		t.Fatalf("only trashed after restore expected 1 got %d", pDel2.Total)
	}
	// purge remaining trashed
	n, err := repo.PurgeTrashed(ctx)
	if err != nil || n != 1 {
		t.Fatalf("purge trashed expected 1, got n=%d err=%v", n, err)
	}
	// with trashed total equals active total (no trashed left)
	all, _ := repo.WithTrashed().FindPage(ctx, kintsnorm.PageRequest{Limit: 10})
	act, _ := repo.FindPage(ctx, kintsnorm.PageRequest{Limit: 10})
	if all.Total != act.Total {
		t.Fatalf("expected totals equal after purge: all=%d act=%d", all.Total, act.Total)
	}
}

func TestSoftDeleteValidationWithoutDeletedAtColumn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// migrate nosoft
	if err := kn.AutoMigrate(&NoSoft{}); err != nil {
		t.Fatalf("migrate nosoft: %v", err)
	}
	repo := kintsnorm.NewRepository[NoSoft](kn)
	if err := repo.Create(ctx, &NoSoft{Name: "n1"}); err != nil {
		t.Fatalf("create nosoft: %v", err)
	}
	// soft delete should error
	if err := repo.SoftDelete(ctx, 1); err == nil {
		t.Fatalf("expected error on soft delete without deleted_at")
	}
	// and bulk too
	if _, err := repo.SoftDeleteAll(ctx); err == nil {
		t.Fatalf("expected error on soft delete all without deleted_at")
	}
	if _, err := repo.PurgeTrashed(ctx); err == nil {
		t.Fatalf("expected error on purge without deleted_at")
	}
}

func TestGetByIDAndNotFound(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _ = kn.Pool().Exec(ctx, "TRUNCATE users RESTART IDENTITY CASCADE")
	repo := kintsnorm.NewRepository[User](kn)
	u := &User{Email: "gid@example.com", Username: "gid", Password: "pw"}
	if err := repo.Create(ctx, u); err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := repo.GetByID(ctx, 1)
	if err != nil || got.Username != "gid" {
		t.Fatalf("getbyid failed: %+v err=%v", got, err)
	}
	if _, err := repo.GetByID(ctx, 99999); err == nil {
		t.Fatalf("expected not found")
	}
}

func TestBatchCreatePartialFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _ = kn.Pool().Exec(ctx, "TRUNCATE users RESTART IDENTITY CASCADE")
	repo := kintsnorm.NewRepository[User](kn)
	// seed one user
	if err := repo.Create(ctx, &User{Email: "seed@example.com", Username: "seed", Password: "x"}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// attempt batch where first conflicts on email, second should not be inserted due to early error
	batch := []*User{
		{Email: "seed@example.com", Username: "dup", Password: "x"},
		{Email: "after@example.com", Username: "after", Password: "x"},
	}
	if err := repo.CreateBatch(ctx, batch); err == nil {
		t.Fatalf("expected error on batch with duplicate")
	}
	if ex, _ := repo.Exists(ctx, kintsnorm.Condition{Expr: "email = ?", Args: []any{"after@example.com"}}); ex {
		t.Fatalf("second batch item should not be inserted after failure")
	}
}

func TestWhereWithMultiplePlaceholdersInOneClause(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _ = kn.Pool().Exec(ctx, "TRUNCATE users RESTART IDENTITY CASCADE")
	repo := kintsnorm.NewRepository[User](kn)
	_ = repo.Create(ctx, &User{Email: "mp1@example.com", Username: "mp1", Password: "x"})
	_ = repo.Create(ctx, &User{Email: "mp2@example.com", Username: "mp2", Password: "x"})
	var out []User
	if err := kn.Query().Table("users").Where("email = ? OR username = ?", "mp1@example.com", "nope").Find(ctx, &out); err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if len(out) != 1 || out[0].Username != "mp1" {
		t.Fatalf("unexpected result: %+v", out)
	}
}

func TestConditionDSLAndStructOps(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _ = kn.Pool().Exec(ctx, "TRUNCATE users RESTART IDENTITY CASCADE")

	// InsertStruct
	u := User{Email: "dsl@example.com", Username: "dsl", Password: "x"}
	if _, err := kn.Query().Table("users").InsertStruct(ctx, &u); err != nil {
		t.Fatalf("insert struct: %v", err)
	}

	// Condition DSL (AND/OR/IN)
	var out []User
	cond := kintsnorm.And(kintsnorm.Eq("email", "dsl@example.com"), kintsnorm.Or(kintsnorm.Eq("username", "dsl"), kintsnorm.In("id", []any{100, 200})))
	if err := kn.Query().Table("users").WhereCond(cond).Find(ctx, &out); err != nil {
		t.Fatalf("dsl find: %v", err)
	}
	if len(out) != 1 || out[0].Username != "dsl" {
		t.Fatalf("unexpected dsl result: %+v", out)
	}

	// UpdateStructByPK placeholder and arg ordering with WHERE
	u.Username = "dsl2"
	if _, err := kn.Query().Table("users").UpdateStructByPK(ctx, &u, "id"); err != nil {
		t.Fatalf("update struct by pk: %v", err)
	}
	var got User
	if err := kn.Query().Table("users").Where("id = ?", u.ID).First(ctx, &got); err != nil {
		t.Fatalf("fetch after update struct: %v", err)
	}
	if got.Username != "dsl2" {
		t.Fatalf("expected username updated via UpdateStructByPK, got %s", got.Username)
	}

	// UpdateStructByPK
	out[0].Password = "y"
	if _, err := kn.Query().Table("users").UpdateStructByPK(ctx, &out[0], "id"); err != nil {
		t.Fatalf("update struct by pk: %v", err)
	}
}

func TestIndexOnProfilesUserIDExists(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := kn.AutoMigrate(&User{}, &Profile{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	var hasIdx bool
	rows, err := kn.Pool().Query(ctx, "select indexname from pg_indexes where schemaname='public' and tablename='profiles'")
	if err != nil {
		t.Fatalf("pg_indexes: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if name == "idx_profiles_user_id" {
			hasIdx = true
		}
	}
	if !hasIdx {
		t.Fatalf("expected idx_profiles_user_id to exist")
	}
}

func TestUpdatePartialEmptyBumpsUpdatedAt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _ = kn.Pool().Exec(ctx, "TRUNCATE users RESTART IDENTITY CASCADE")
	repo := kintsnorm.NewRepository[User](kn)
	if err := repo.Create(ctx, &User{Email: "emptyu@example.com", Username: "emptyu", Password: "x"}); err != nil {
		t.Fatalf("create: %v", err)
	}
	g1, err := repo.FindOne(ctx, kintsnorm.Condition{Expr: "email = ?", Args: []any{"emptyu@example.com"}})
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	before := g1.UpdatedAt
	// call UpdatePartial with no fields: should bump updated_at due to on_update rule
	if err := repo.UpdatePartial(ctx, g1.ID, map[string]any{}); err != nil {
		t.Fatalf("partial empty: %v", err)
	}
	g2, err := repo.FindOne(ctx, kintsnorm.Condition{Expr: "id = ?", Args: []any{g1.ID}})
	if err != nil {
		t.Fatalf("find2: %v", err)
	}
	if !g2.UpdatedAt.After(before) {
		t.Fatalf("expected updated_at bump on empty partial update")
	}
}

// --- Migration diff and quoting tests ---

// Rename -> renames
type Rename struct {
	ID   int64  `db:"id" norm:"primary_key,auto_increment"`
	Name string `db:"display_name" norm:"rename:name,text"`
}

// TypeTest -> type_tests
type TypeTest struct {
	ID   int64  `db:"id" norm:"primary_key,auto_increment"`
	Age  int64  `db:"age"`
	Name string `db:"name" norm:"not_null"`
}

// Quoted -> quoteds
type Quoted struct {
	ID    int64  `db:"id" norm:"primary_key,auto_increment"`
	Order string `db:"order" norm:"varchar(50),index"`
}

func TestMigrationPlanRenameAndTypeNullabilityWarnings(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Prepare schema for rename: create table renames(name text)
	_, _ = kn.Pool().Exec(ctx, `DROP TABLE IF EXISTS renames`)
	if _, err := kn.Pool().Exec(ctx, `CREATE TABLE renames (id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, name TEXT)`); err != nil {
		t.Fatalf("create renames: %v", err)
	}
	// Prepare schema for type/nullability: create table type_tests(age integer, name text NULL)
	_, _ = kn.Pool().Exec(ctx, `DROP TABLE IF EXISTS type_tests`)
	if _, err := kn.Pool().Exec(ctx, `CREATE TABLE type_tests (id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, age INTEGER, name TEXT NULL)`); err != nil {
		t.Fatalf("create type_tests: %v", err)
	}

	mg := migration.NewMigrator(kn.Pool())
	plan1, err := mg.Plan(ctx, &Rename{})
	if err != nil {
		t.Fatalf("plan rename: %v", err)
	}
	// Should include a rename column statement
	foundRename := false
	for _, s := range plan1.Statements {
		if strings.Contains(s, `ALTER TABLE "renames" RENAME COLUMN "name" TO "display_name"`) {
			foundRename = true
			break
		}
	}
	if !foundRename {
		t.Fatalf("expected rename column statement, got: %#v", plan1)
	}

	// Apply AutoMigrate and verify column renamed
	if err := kn.AutoMigrate(&Rename{}); err != nil {
		t.Fatalf("automigrate rename: %v", err)
	}
	var cnt int
	if err := kn.Pool().QueryRow(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_name='renames' AND column_name='display_name'`).Scan(&cnt); err != nil {
		t.Fatalf("check column: %v", err)
	}
	if cnt != 1 {
		t.Fatalf("display_name not found after migrate")
	}
	if err := kn.Pool().QueryRow(ctx, `SELECT count(*) FROM information_schema.columns WHERE table_name='renames' AND column_name='name'`).Scan(&cnt); err != nil {
		t.Fatalf("check old column: %v", err)
	}
	if cnt != 0 {
		t.Fatalf("old column name still exists after rename")
	}

	// Plan for type/nullability should produce warnings and unsafe statements
	plan2, err := mg.Plan(ctx, &TypeTest{})
	if err != nil {
		t.Fatalf("plan type/null: %v", err)
	}
	hasTypeWarn := false
	hasNullWarn := false
	for _, w := range plan2.Warnings {
		if strings.Contains(strings.ToLower(w), "type change") {
			hasTypeWarn = true
		}
		if strings.Contains(strings.ToLower(w), "nullability change") {
			hasNullWarn = true
		}
	}
	if !hasTypeWarn || !hasNullWarn {
		t.Fatalf("expected type and nullability warnings, got: %#v", plan2.Warnings)
	}
	if len(plan2.UnsafeStatements) == 0 {
		t.Fatalf("expected unsafe statements for type/null changes")
	}
}

func TestMigrationPlanFormatting(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	mg := migration.NewMigrator(kn.Pool())
	plan, err := mg.Plan(ctx, &Rename{}, &TypeTest{})
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	out := migration.FormatPlan(plan)
	// should contain header
	if !strings.Contains(out, "Migration Plan") {
		t.Fatalf("expected formatted output header, got: \n%s", out)
	}
	// should contain at least one section for a table or schema_migrations
	if !(strings.Contains(out, "[type_tests]") || strings.Contains(out, "[schema_migrations]")) {
		t.Fatalf("expected formatted output with table sections, got: \n%s", out)
	}
	// if warnings present, ensure section exists
	if len(plan.Warnings) > 0 && !strings.Contains(out, "Warnings:") {
		t.Fatalf("expected warnings section in formatted output")
	}
}

func TestIdentifierQuotingOnReservedColumn(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// Migrate quoteds with reserved column name "order"
	if err := kn.AutoMigrate(&Quoted{}); err != nil {
		t.Fatalf("migrate quoted: %v", err)
	}
	// Insert and read
	if _, err := kn.Pool().Exec(ctx, `INSERT INTO quoteds("order") VALUES ($1)`, "abc"); err != nil {
		t.Fatalf("insert quoted: %v", err)
	}
	var got string
	if err := kn.Pool().QueryRow(ctx, `SELECT "order" FROM quoteds`).Scan(&got); err != nil {
		t.Fatalf("select quoted: %v", err)
	}
	if got != "abc" {
		t.Fatalf("unexpected value: %s", got)
	}
}

func TestNamedParametersInBuilderAndRaw(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _ = kn.Pool().Exec(ctx, "TRUNCATE users RESTART IDENTITY CASCADE")
	// seed
	_, _ = kn.Pool().Exec(ctx, "INSERT INTO users(email, username, password) VALUES ($1,$2,$3),($4,$5,$6)",
		"np1@example.com", "np1", "x",
		"np2@example.com", "np2", "x",
	)

	// WhereNamed with scalar and repeated usage
	var out []User
	cond := "email = :email AND username <> :email" // intentionally reuse name
	if err := kn.Query().Table("users").WhereNamed(cond, map[string]any{"email": "np1@example.com"}).Find(ctx, &out); err != nil {
		t.Fatalf("where named: %v", err)
	}
	if len(out) != 1 || out[0].Username != "np1" {
		t.Fatalf("unexpected where named: %+v", out)
	}

	// IN (...) expansion via named slice
	out = nil
	if err := kn.Query().Table("users").WhereNamed("username IN :names", map[string]any{"names": []string{"np1", "np2"}}).Find(ctx, &out); err != nil {
		t.Fatalf("where named slice: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 via IN slice, got %d", len(out))
	}

	// RawNamed with cast and string literal containing ':'
	var rows []map[string]any
	err := kn.Query().RawNamed("select (:a::int + :b::int) as s, ':literal' as l", map[string]any{"a": 2, "b": 3}).Find(ctx, &rows)
	if err != nil {
		t.Fatalf("raw named: %v", err)
	}
	if fmt.Sprint(rows[0]["s"]) != "5" || fmt.Sprint(rows[0]["l"]) != ":literal" {
		t.Fatalf("unexpected raw named result: %+v", rows)
	}
}

func TestIdentifierQuotingHelpersInBuilder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// create table with reserved and mixed-case identifiers
	_, _ = kn.Pool().Exec(ctx, `DROP TABLE IF EXISTS qhelpers`)
	if _, err := kn.Pool().Exec(ctx, `CREATE TABLE qhelpers ("Order" TEXT, "user.name" TEXT)`); err != nil {
		t.Fatalf("create qhelpers: %v", err)
	}
	// insert using raw to seed
	if _, err := kn.Pool().Exec(ctx, `INSERT INTO qhelpers("Order", "user.name") VALUES ($1,$2)`, "A1", "bob"); err != nil {
		t.Fatalf("seed qhelpers: %v", err)
	}
	// select via builder quoting helpers
	var rows []map[string]any
	if err := kn.Query().TableQ("qhelpers").SelectQI("Order", "user.name").Find(ctx, &rows); err != nil {
		t.Fatalf("select qhelpers: %v", err)
	}
	if len(rows) != 1 || fmt.Sprint(rows[0]["Order"]) != "A1" || fmt.Sprint(rows[0]["user.name"]) != "bob" {
		t.Fatalf("unexpected qhelpers rows: %+v", rows)
	}
}

func TestEagerAndLazyLoadHelpers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// migrate and seed
	if err := kn.AutoMigrate(&User{}, &Profile{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	_, _ = kn.Pool().Exec(ctx, "TRUNCATE users RESTART IDENTITY CASCADE")
	_, _ = kn.Pool().Exec(ctx, "TRUNCATE profiles RESTART IDENTITY CASCADE")
	// parents
	_, _ = kn.Pool().Exec(ctx, "INSERT INTO users(email, username, password) VALUES ($1,$2,$3),($4,$5,$6)",
		"el1@example.com", "el1", "x",
		"el2@example.com", "el2", "x",
	)
	// children: 2 for user 1, 1 for user 2
	_, _ = kn.Pool().Exec(ctx, "INSERT INTO profiles(user_id, bio) VALUES ($1,$2),($3,$4),($5,$6)",
		1, "p1-1",
		1, "p1-2",
		2, "p2-1",
	)
	// load parents via repo
	repo := kintsnorm.NewRepository[User](kn)
	parents, err := repo.Find(ctx)
	if err != nil || len(parents) != 2 {
		t.Fatalf("load parents: %+v err=%v", parents, err)
	}
	// add a field on the fly using a holder struct local to this test to receive children
	type userWithProfiles struct {
		User
		Profiles []*Profile
	}
	ups := []*userWithProfiles{{User: *parents[0]}, {User: *parents[1]}}
	// Eager load
	set := func(p *userWithProfiles, children []*Profile) { p.Profiles = children }
	getID := func(p *userWithProfiles) any { return p.ID }
	if err := kintsnorm.EagerLoadMany[userWithProfiles, Profile](ctx, kn, ups, getID, "user_id", set); err != nil {
		t.Fatalf("eager load: %v", err)
	}
	if len(ups[0].Profiles) != 2 || len(ups[1].Profiles) != 1 {
		t.Fatalf("unexpected grouping: %+v %+v", ups[0].Profiles, ups[1].Profiles)
	}
	// Lazy load
	lazy, err := kintsnorm.LazyLoadMany[Profile](ctx, kn, 1, "user_id")
	if err != nil || len(lazy) != 2 {
		t.Fatalf("lazy load: %v len=%d", err, len(lazy))
	}
}

// --- Cache hooks e2e ---
type memCache struct{ store atomic.Value }

func newMemCache() *memCache {
	m := &memCache{}
	m.store.Store(map[string][]byte{})
	return m
}

func (m *memCache) Get(ctx context.Context, key string) ([]byte, bool, error) {
	mp := m.store.Load().(map[string][]byte)
	v, ok := mp[key]
	return v, ok, nil
}
func (m *memCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	old := m.store.Load().(map[string][]byte)
	cp := make(map[string][]byte, len(old)+1)
	for k, v := range old {
		cp[k] = v
	}
	cp[key] = value
	m.store.Store(cp)
	return nil
}
func (m *memCache) Invalidate(ctx context.Context, keys ...string) error {
	old := m.store.Load().(map[string][]byte)
	cp := make(map[string][]byte, len(old))
	for k, v := range old {
		cp[k] = v
	}
	for _, k := range keys {
		delete(cp, k)
	}
	m.store.Store(cp)
	return nil
}

func TestCacheReadThroughAndInvalidation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	host := getenvDefault("PGHOST", "127.0.0.1")
	port := getenvDefault("PGPORT", "5432")
	user := getenvDefault("PGUSER", "postgres")
	pass := getenvDefault("PGPASSWORD", "postgres")
	db := getenvDefault("PGDATABASE", "postgres")

	dsn := fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=disable", host, port, db, user, pass)
	cache := newMemCache()
	knc, err := kintsnorm.NewWithConnString(dsn, kintsnorm.WithCache(cache))
	if err != nil {
		t.Fatalf("new with cache: %v", err)
	}
	defer func() { _ = knc.Close() }()

	_, _ = knc.Pool().Exec(ctx, "TRUNCATE users RESTART IDENTITY CASCADE")
	// seed
	_, _ = knc.Pool().Exec(ctx, "INSERT INTO users(email, username, password) VALUES ($1,$2,$3)", "c1@example.com", "c1", "x")

	key := "users:by_email:c1@example.com"
	var rows []map[string]any
	// miss -> load -> set
	if err := knc.Query().Table("users").Select("email", "username").Where("email = ?", "c1@example.com").WithCacheKey(key, 5*time.Minute).Find(ctx, &rows); err != nil {
		t.Fatalf("find with cache: %v", err)
	}
	if _, ok, _ := cache.Get(ctx, key); !ok {
		t.Fatalf("expected cache set after read-through")
	}
	// write -> invalidate
	if _, err := knc.Pool().Exec(ctx, "UPDATE users SET username=$1 WHERE email=$2", "c1x", "c1@example.com"); err != nil {
		t.Fatalf("update: %v", err)
	}
	// simulate invalidation via builder hook
	if err := knc.Query().Raw("SELECT 1").WithInvalidateKeys(key).Exec(ctx); err != nil {
		t.Fatalf("invalidate exec: %v", err)
	}
	if _, ok, _ := cache.Get(ctx, key); ok {
		t.Fatalf("expected cache invalidated")
	}
}

func TestErrorMapping_DuplicateAndFKViolation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, _ = kn.Pool().Exec(ctx, "TRUNCATE users RESTART IDENTITY CASCADE")
	// create fk table manually (migration engine lacks IF NOT EXISTS for ADD CONSTRAINT)
	if _, err := kn.Pool().Exec(ctx, `DROP TABLE IF EXISTS fk_posts`); err != nil {
		t.Fatalf("drop fk_posts: %v", err)
	}
	create := `CREATE TABLE fk_posts (
        id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
        user_id BIGINT NOT NULL REFERENCES users(id),
        body TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )`
	if _, err := kn.Pool().Exec(ctx, create); err != nil {
		t.Fatalf("create fk_posts: %v", err)
	}
	// duplicate unique on users.email
	if _, err := kn.Pool().Exec(ctx, "INSERT INTO users(email, username, password) VALUES ($1,$2,$3)", "dup@example.com", "dup", "x"); err != nil {
		t.Fatalf("seed dup: %v", err)
	}
	_, err := kn.Pool().Exec(ctx, "INSERT INTO users(email, username, password) VALUES ($1,$2,$3)", "dup@example.com", "dup2", "x")
	if err == nil {
		t.Fatalf("expected duplicate error")
	}
	// run through builder.Raw to get wrapPgError
	err = kn.Query().Raw("INSERT INTO users(email, username, password) VALUES(?,?,?)", "dup@example.com", "dup3", "x").Exec(ctx)
	if err == nil {
		t.Fatalf("expected duplicate via builder")
	}
	var ormErr *kintsnorm.ORMError
	if !errors.As(err, &ormErr) || ormErr.Code != kintsnorm.ErrCodeDuplicate {
		t.Fatalf("expected ErrCodeDuplicate, got %#v", err)
	}
	// FK violation: insert FKPost with unknown user_id
	_, err = kn.Pool().Exec(ctx, "INSERT INTO fk_posts(user_id, body) VALUES ($1,$2)", 99999, "x")
	if err == nil {
		t.Fatalf("expected FK violation")
	}
	// through builder for wrap
	err = kn.Query().Raw("INSERT INTO fk_posts(user_id, body) VALUES(?,?)", 99999, "y").Exec(ctx)
	if err == nil || !errors.As(err, &ormErr) || ormErr.Code != kintsnorm.ErrCodeConstraint {
		t.Fatalf("expected constraint code, got %#v", err)
	}
}

func TestFKCascadeDelete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := kn.AutoMigrate(&CascadeParent{}, &CascadeChild{}); err != nil {
		t.Fatalf("migrate cascade: %v", err)
	}
	_, _ = kn.Pool().Exec(ctx, "TRUNCATE cascade_childs RESTART IDENTITY CASCADE")
	_, _ = kn.Pool().Exec(ctx, "TRUNCATE cascade_parents RESTART IDENTITY CASCADE")
	if _, err := kn.Pool().Exec(ctx, `INSERT INTO cascade_parents(name) VALUES ($1)`, "p1"); err != nil {
		t.Fatalf("seed parent: %v", err)
	}
	if _, err := kn.Pool().Exec(ctx, `INSERT INTO cascade_childs(parent_id) VALUES ($1)`, 1); err != nil {
		t.Fatalf("seed child: %v", err)
	}
	if _, err := kn.Pool().Exec(ctx, `DELETE FROM cascade_parents WHERE id=$1`, 1); err != nil {
		t.Fatalf("delete parent: %v", err)
	}
	var cnt int
	if err := kn.Pool().QueryRow(ctx, `SELECT COUNT(*) FROM cascade_childs`).Scan(&cnt); err != nil {
		t.Fatalf("count children: %v", err)
	}
	if cnt != 0 {
		t.Fatalf("expected cascade delete to remove children, got %d", cnt)
	}
}

func TestManualMigrationsUpDown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// ensure clean start
	_, _ = kn.Pool().Exec(ctx, `DROP TABLE IF EXISTS manual_e2e`)

	dir := t.TempDir()
	mustWrite := func(name, sql string) {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(sql), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	// Use high version numbers to avoid collision with automigrate
	mustWrite("1000001_init.up.sql", `CREATE TABLE manual_e2e (id BIGINT PRIMARY KEY, name TEXT)`)
	mustWrite("1000001_init.down.sql", `DROP TABLE manual_e2e`)
	mustWrite("1000002_addcol.up.sql", `ALTER TABLE manual_e2e ADD COLUMN age INTEGER`)
	mustWrite("1000002_addcol.down.sql", `ALTER TABLE manual_e2e DROP COLUMN age`)

	// capture migration count before
	var before int
	if err := kn.Pool().QueryRow(ctx, `SELECT COUNT(*) FROM schema_migrations`).Scan(&before); err != nil {
		// table might not exist yet; create it lazily by running a no-op automigrate
		_ = kn.AutoMigrate(&User{})
		if err2 := kn.Pool().QueryRow(ctx, `SELECT COUNT(*) FROM schema_migrations`).Scan(&before); err2 != nil {
			t.Fatalf("schema_migrations count before: %v", err2)
		}
	}

	if err := kn.MigrateUpDir(ctx, dir); err != nil {
		t.Fatalf("migrate up dir failed: %v", err)
	}

	// table exists
	var reg *string
	if err := kn.Pool().QueryRow(ctx, `select to_regclass('public.manual_e2e')`).Scan(&reg); err != nil {
		t.Fatalf("regclass manual_e2e: %v", err)
	}
	if reg == nil || *reg != "manual_e2e" {
		t.Fatalf("manual_e2e table not found")
	}
	// age column exists
	var c int
	if err := kn.Pool().QueryRow(ctx, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='manual_e2e' AND column_name='age'`).Scan(&c); err != nil {
		t.Fatalf("check age column: %v", err)
	}
	if c != 1 {
		t.Fatalf("expected age column to exist, got %d", c)
	}
	// migration rows increased by 2
	var afterUp int
	if err := kn.Pool().QueryRow(ctx, `SELECT COUNT(*) FROM schema_migrations`).Scan(&afterUp); err != nil {
		t.Fatalf("schema_migrations after up: %v", err)
	}
	if afterUp < before+2 {
		t.Fatalf("expected at least +2 migration rows, before=%d after=%d", before, afterUp)
	}

	// safety: default blocks DROP COLUMN/TABLE
	if err := kn.MigrateDownDir(ctx, dir, 1); err == nil {
		t.Fatalf("expected safety gate to block DROP COLUMN by default")
	}
	// allow column drop and try again
	kn.SetManualMigrationOptions(migration.ManualOptions{AllowColumnDrop: true})
	if err := kn.MigrateDownDir(ctx, dir, 1); err != nil {
		t.Fatalf("migrate down 1 failed with AllowColumnDrop: %v", err)
	}
	if err := kn.Pool().QueryRow(ctx, `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='public' AND table_name='manual_e2e' AND column_name='age'`).Scan(&c); err != nil {
		t.Fatalf("check age after down: %v", err)
	}
	if c != 0 {
		t.Fatalf("expected age column dropped, got %d", c)
	}

	// down second step -> drop table, but blocked until allowed
	if err := kn.MigrateDownDir(ctx, dir, 1); err == nil {
		t.Fatalf("expected safety gate to block DROP TABLE by default")
	}
	kn.SetManualMigrationOptions(migration.ManualOptions{AllowTableDrop: true, AllowColumnDrop: true})
	if err := kn.MigrateDownDir(ctx, dir, 1); err != nil {
		t.Fatalf("migrate down 2 failed with AllowTableDrop: %v", err)
	}
	if err := kn.Pool().QueryRow(ctx, `select to_regclass('public.manual_e2e')`).Scan(&reg); err != nil {
		t.Fatalf("regclass after full down: %v", err)
	}
	if reg != nil {
		t.Fatalf("expected manual_e2e to be dropped, reg=%v", *reg)
	}
}

func getenvDefault(k, def string) string {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	return v
}

func waitTCP(host, port string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	addr := net.JoinHostPort(host, port)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 1*time.Second)
		if err == nil {
			_ = c.Close()
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %s:%s", host, port)
}
