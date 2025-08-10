package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	kintsnorm "github.com/kintsdev/norm"
)

// Verifies that QueryRead() uses the read-only pool when ReadOnlyConnString is configured
func TestReadPool_QueryReadUsesReadPool(t *testing.T) {
	host := getenvDefault("PGHOST", "127.0.0.1")
	port := getenvDefault("PGPORT", "5432")
	user := getenvDefault("PGUSER", "postgres")
	pass := getenvDefault("PGPASSWORD", "postgres")
	db := getenvDefault("PGDATABASE", "postgres")

	if err := waitTCP(host, port, 30*time.Second); err != nil {
		t.Fatalf("postgres not reachable: %v", err)
	}

	primaryApp := "kints-norm-primary-e2e"
	readApp := "kints-norm-read-e2e"

	// Build read-only DSN with a distinct application_name
	readOnlyDSN := fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=disable application_name=%s", host, port, db, user, pass, readApp)

	cfg := &kintsnorm.Config{
		Host:               host,
		Database:           db,
		Username:           user,
		Password:           pass,
		SSLMode:            "disable",
		ApplicationName:    primaryApp,
		ReadOnlyConnString: readOnlyDSN,
	}

	kn2, err := kintsnorm.New(cfg)
	if err != nil {
		t.Fatalf("new norm: %v", err)
	}
	defer func() { _ = kn2.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Normal Query() should hit primary (application_name = primaryApp)
	var gotPrimary []map[string]any
	if err := kn2.Query().Raw("select current_setting('application_name') as app").Find(ctx, &gotPrimary); err != nil {
		t.Fatalf("query primary app: %v", err)
	}
	if len(gotPrimary) != 1 || fmt.Sprint(gotPrimary[0]["app"]) != primaryApp {
		t.Fatalf("expected primary app %s, got: %+v", primaryApp, gotPrimary)
	}

	// QueryRead() should hit read pool (application_name = readApp)
	var gotRead []map[string]any
	if err := kn2.QueryRead().Raw("select current_setting('application_name') as app").Find(ctx, &gotRead); err != nil {
		t.Fatalf("query read app: %v", err)
	}
	if len(gotRead) != 1 || fmt.Sprint(gotRead[0]["app"]) != readApp {
		t.Fatalf("expected read app %s, got: %+v", readApp, gotRead)
	}
}
