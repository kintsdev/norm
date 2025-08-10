package main

import (
	"context"
	"log"

	"github.com/kintsdev/norm"
)

// Ignore demo: field with norm:"-" is not migrated
type Doc struct {
	ID   int64  `db:"id" norm:"primary_key,auto_increment"`
	Name string `db:"name"`
	Temp string `db:"temp" norm:"-"`
}

func main() {
	cfg := &norm.Config{Host: "127.0.0.1", Port: 5432, Database: "postgres", Username: "postgres", Password: "postgres", SSLMode: "disable"}
	kn, err := norm.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer kn.Close()

	if err := kn.AutoMigrate(&Doc{}); err != nil {
		log.Fatal(err)
	}

	// only id, name exist
	_, _ = kn.Pool().Exec(context.Background(), `INSERT INTO docs(name) VALUES ($1)`, "x")
}
