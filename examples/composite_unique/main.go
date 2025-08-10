package main

import (
	"context"
	"log"

	"github.com/kintsdev/norm"
)

// Account demonstrates composite unique with optional unique_name
type Account struct {
	ID       int64  `db:"id" norm:"primary_key,auto_increment"`
	TenantID int64  `db:"tenant_id" norm:"not_null,unique:tenant_slug"`
	Slug     string `db:"slug" norm:"not_null,unique:tenant_slug,unique_name:uq_accounts_tenant_slug"`
}

func main() {
	cfg := &norm.Config{Host: "127.0.0.1", Port: 5432, Database: "postgres", Username: "postgres", Password: "postgres", SSLMode: "disable"}
	kn, err := norm.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer kn.Close()

	if err := kn.AutoMigrate(&Account{}); err != nil {
		log.Fatal(err)
	}

	// First insert ok
	if _, err := kn.Pool().Exec(context.Background(), `INSERT INTO accounts(tenant_id, slug) VALUES ($1,$2)`, 1, "demo"); err != nil {
		log.Fatal(err)
	}
	// Second with same unique group should fail
	if _, err := kn.Pool().Exec(context.Background(), `INSERT INTO accounts(tenant_id, slug) VALUES ($1,$2)`, 1, "demo"); err == nil {
		log.Fatal("expected unique violation")
	}
}
