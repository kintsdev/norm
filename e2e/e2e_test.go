package e2e

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	kintsnorm "kints-norm"
	migration "kints-norm/migration"
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

type Profile struct {
	ID        int64     `db:"id" orm:"primary_key,auto_increment"`
	UserID    int64     `db:"user_id" orm:"index,not_null"`
	Bio       string    `db:"bio" orm:"varchar(255)"`
	CreatedAt time.Time `db:"created_at" orm:"not_null,default:now()"`
}

// Model without soft delete column to validate errors
type NoSoft struct {
	ID        int64     `db:"id" orm:"primary_key,auto_increment"`
	Name      string    `db:"name" orm:"not_null,varchar(100)"`
	CreatedAt time.Time `db:"created_at" orm:"not_null,default:now()"`
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

	if err := kn.AutoMigrate(&User{}, &Profile{}); err != nil {
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
	if err := repo.SoftDelete(ctx, got.ID); err != nil {
		t.Fatalf("soft delete: %v", err)
	}

	// DeletedAt should be set; verify
	var deletedAt *time.Time
	if err := kn.Pool().QueryRow(ctx, "select deleted_at from users where id = $1", got.ID).Scan(&deletedAt); err != nil {
		t.Fatalf("query deleted_at: %v", err)
	}
	if deletedAt == nil {
		t.Fatalf("deleted_at not set")
	}

	// Default Find should not return soft-deleted rows
	if ex, err := repo.Exists(ctx, kintsnorm.Condition{Expr: "id = ?", Args: []any{got.ID}}); err != nil {
		t.Fatalf("exists error: %v", err)
	} else if ex {
		t.Fatalf("soft-deleted row should be hidden in Exists/Count")
	}
	if _, err := repo.FindOne(ctx, kintsnorm.Condition{Expr: "id = ?", Args: []any{got.ID}}); err == nil {
		t.Fatalf("soft-deleted row should not be returned by FindOne")
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

	// join query
	var joined []map[string]any
	if err := kn.Query().Table("users u").Join("profiles p", "u.id = p.user_id").Select("u.id", "p.bio").Find(ctx, &joined); err != nil {
		t.Fatalf("join find: %v", err)
	}
	if len(joined) != 1 || fmt.Sprint(joined[0]["bio"]) != "bio-1" {
		t.Fatalf("unexpected join result: %+v", joined)
	}

	// Raw with placeholders
	var calc []map[string]any
	if err := kn.Query().Raw("select ?::int + ?::int as sum", 2, 3).Find(ctx, &calc); err != nil {
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

	// Delete using builder
	aff, err := kn.Query().Table("users").Where("username = ?", "f2").Delete(ctx)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if aff != 1 {
		t.Fatalf("expected 1 row deleted, got %d", aff)
	}

	// Ensure f2 gone
	var rows []User
	if err := kn.Query().Table("users").Where("username = ?", "f2").Find(ctx, &rows); err != nil {
		t.Fatalf("find: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected no rows for f2, got %d", len(rows))
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
	ID   int64  `db:"id" orm:"primary_key,auto_increment"`
	Name string `db:"display_name" orm:"rename:name,text"`
}

// TypeTest -> type_tests
type TypeTest struct {
	ID   int64  `db:"id" orm:"primary_key,auto_increment"`
	Age  int64  `db:"age"`
	Name string `db:"name" orm:"not_null"`
}

// Quoted -> quoteds
type Quoted struct {
	ID    int64  `db:"id" orm:"primary_key,auto_increment"`
	Order string `db:"order" orm:"varchar(50),index"`
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
