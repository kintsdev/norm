package main

import (
	"context"
	"log"
	"time"

	"github.com/kintsdev/norm"
)

// PartialIdx demonstrates partial index with index_where and using
type PartialIdx struct {
	ID        int64      `db:"id" norm:"primary_key,auto_increment"`
	Email     string     `db:"email" norm:"index,using:gin,index_where:(deleted_at IS NULL)"`
	DeletedAt *time.Time `db:"deleted_at"`
}

func main() {
	cfg := &norm.Config{Host: "127.0.0.1", Port: 5432, Database: "postgres", Username: "postgres", Password: "postgres", SSLMode: "disable"}
	kn, err := norm.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer kn.Close()

	if err := kn.AutoMigrate(&PartialIdx{}); err != nil {
		log.Fatal(err)
	}

	// Sample write
	if _, err := kn.Pool().Exec(context.Background(), `INSERT INTO partial_idxs(email) VALUES ($1)`, "p@example.com"); err != nil {
		log.Fatal(err)
	}
}
