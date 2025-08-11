package main

import (
	"context"
	"log"
	"os"

	"github.com/kintsdev/norm"
	"github.com/kintsdev/norm/migration"
)

func main() {
	ctx := context.Background()

	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
	}

	db, err := norm.NewWithConnString(dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	db.SetManualMigrationOptions(migration.ManualOptions{AllowTableDrop: false, AllowColumnDrop: false})

	if err := db.MigrateUpDir(ctx, "./migrations"); err != nil {
		log.Fatal("migration error: ", err)
	}

	_, _ = db.Pool().Exec(ctx, `INSERT INTO users(email, username, password) VALUES('m1@example.com','m1','pw') ON CONFLICT DO NOTHING`)

	// To rollback last migration step, uncomment below:
	// if err := db.MigrateDownDir(ctx, "./migrations", 1); err != nil {
	//     log.Fatal(err)
	// }
}
