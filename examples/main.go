package main

import (
	"context"
	"log"

	kintsnorm "github.com/kintsdev/norm"
)

func main() {
	ctx := context.Background()
	cfg := &kintsnorm.Config{
		Host:           "localhost",
		Port:           5432,
		Database:       "postgres",
		Username:       "postgres",
		Password:       "postgres",
		MaxConnections: 5,
		MinConnections: 1,
	}

	db, err := kintsnorm.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := db.Health(ctx); err != nil {
		log.Fatal(err)
	}

	var rows []map[string]any
	if err := db.Query().Table("pg_catalog.pg_tables").Select("schemaname", "tablename").Limit(3).Find(ctx, &rows); err != nil {
		log.Fatal(err)
	}
	log.Println("tables:", rows)
}
