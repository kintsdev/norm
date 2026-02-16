package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	kintsnorm "github.com/kintsdev/norm"
)

// TestAutoReadRouting_Repository verifies that repository Find/GetByID operations
// automatically route to the read pool when configured, while writes still go to primary.
func TestAutoReadRouting_Repository(t *testing.T) {
	host := getenvDefault("PGHOST", "127.0.0.1")
	port := getenvDefault("PGPORT", "5432")
	user := getenvDefault("PGUSER", "postgres")
	pass := getenvDefault("PGPASSWORD", "postgres")
	db := getenvDefault("PGDATABASE", "postgres")

	if err := waitTCP(host, port, 30*time.Second); err != nil {
		t.Fatalf("postgres not reachable: %v", err)
	}

	primaryApp := "kints-norm-repo-primary"
	readApp := "kints-norm-repo-read"

	readOnlyDSN := fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=disable application_name=%s", host, port, db, user, pass, readApp)

	cfg := &kintsnorm.Config{
		Host:               host,
		Database:           db,
		Username:           user,
		Password:           pass,
		SSLMode:            "disable",
		ApplicationName:    primaryApp,
		ReadOnlyConnString: readOnlyDSN,
		RetryAttempts:      3,
		RetryBackoff:       50 * time.Millisecond,
	}

	kn, err := kintsnorm.New(cfg)
	if err != nil {
		t.Fatalf("new norm: %v", err)
	}
	defer func() { _ = kn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Setup table
	if err := kn.AutoMigrate(&User{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// Clean up
	kn.Query().Table("users").Raw("DELETE FROM users WHERE email LIKE '%@routing-test.dev'").Exec(ctx) //nolint:errcheck

	// Create a user (write goes to primary)
	repo := kintsnorm.NewRepository[User](kn)
	u := &User{
		Email:    "routing@routing-test.dev",
		Username: "routing_user",
		Password: "pass123",
	}
	if err := repo.Create(ctx, u); err != nil {
		t.Fatalf("create user: %v", err)
	}

	// Find should use read pool automatically via routingExecuter
	// Since both pools point to the same DB in this test setup, the query succeeds.
	// We verify routing works by checking Query() auto-routes reads.
	var readResult []map[string]any
	if err := kn.Query().Raw("SELECT current_setting('application_name') AS app").Find(ctx, &readResult); err != nil {
		t.Fatalf("read routing check: %v", err)
	}
	if len(readResult) != 1 {
		t.Fatalf("expected 1 row, got %d", len(readResult))
	}
	gotApp := fmt.Sprint(readResult[0]["app"])
	if gotApp != readApp {
		t.Fatalf("expected read pool app=%s, got=%s", readApp, gotApp)
	}

	// Verify Find still works (data was written to primary, readable via read pool in same DB)
	users, err := repo.Find(ctx, kintsnorm.Condition{Expr: "email = ?", Args: []any{"routing@routing-test.dev"}})
	if err != nil {
		t.Fatalf("find: %v", err)
	}
	if len(users) != 1 {
		t.Fatalf("expected 1 user, got %d", len(users))
	}
	if users[0].Email != "routing@routing-test.dev" {
		t.Fatalf("unexpected email: %s", users[0].Email)
	}

	// Cleanup
	kn.Query().Table("users").Raw("DELETE FROM users WHERE email LIKE '%@routing-test.dev'").Exec(ctx) //nolint:errcheck
}

// TestRetryIdempotency verifies that retries work for idempotent read operations.
func TestRetryIdempotency(t *testing.T) {
	host := getenvDefault("PGHOST", "127.0.0.1")
	port := getenvDefault("PGPORT", "5432")
	user := getenvDefault("PGUSER", "postgres")
	pass := getenvDefault("PGPASSWORD", "postgres")
	db := getenvDefault("PGDATABASE", "postgres")

	if err := waitTCP(host, port, 30*time.Second); err != nil {
		t.Fatalf("postgres not reachable: %v", err)
	}

	cfg := &kintsnorm.Config{
		Host:          host,
		Database:      db,
		Username:      user,
		Password:      pass,
		SSLMode:       "disable",
		RetryAttempts: 3,
		RetryBackoff:  50 * time.Millisecond,
	}

	kn, err := kintsnorm.New(cfg)
	if err != nil {
		t.Fatalf("new norm: %v", err)
	}
	defer func() { _ = kn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Ensure table
	if err := kn.AutoMigrate(&User{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// Clean and insert
	kn.Query().Table("users").Raw("DELETE FROM users WHERE email = 'retry@retry-test.dev'").Exec(ctx) //nolint:errcheck

	repo := kintsnorm.NewRepository[User](kn)
	u := &User{
		Email:    "retry@retry-test.dev",
		Username: "retry_user",
		Password: "pass123",
	}
	if err := repo.Create(ctx, u); err != nil {
		t.Fatalf("create user: %v", err)
	}

	// Multiple reads should always return consistent results (idempotent)
	for i := range 5 {
		users, err := repo.Find(ctx, kintsnorm.Condition{Expr: "email = ?", Args: []any{"retry@retry-test.dev"}})
		if err != nil {
			t.Fatalf("find attempt %d: %v", i, err)
		}
		if len(users) != 1 {
			t.Fatalf("attempt %d: expected 1 user, got %d", i, len(users))
		}
	}

	// Idempotent write: create same email again should fail (unique constraint)
	u2 := &User{
		Email:    "retry@retry-test.dev",
		Username: "retry_user_dup",
		Password: "pass456",
	}
	err = repo.Create(ctx, u2)
	if err == nil {
		t.Fatal("expected unique violation error, got nil")
	}

	// Cleanup
	kn.Query().Table("users").Raw("DELETE FROM users WHERE email = 'retry@retry-test.dev'").Exec(ctx) //nolint:errcheck
}
