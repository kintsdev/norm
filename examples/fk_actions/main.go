package main

import (
	"context"
	"log"

	"github.com/kintsdev/norm"
)

type Parent struct {
	ID   int64  `db:"id" norm:"primary_key,auto_increment"`
	Name string `db:"name"`
}

type Child struct {
	ID       int64 `db:"id" norm:"primary_key,auto_increment"`
	ParentID int64 `db:"parent_id" norm:"not_null,fk:parents(id),on_delete:cascade,fk_name:fk_child_parent"`
}

func main() {
	cfg := &norm.Config{Host: "127.0.0.1", Port: 5432, Database: "postgres", Username: "postgres", Password: "postgres", SSLMode: "disable"}
	kn, err := norm.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer kn.Close()

	if err := kn.AutoMigrate(&Parent{}, &Child{}); err != nil {
		log.Fatal(err)
	}

	_, _ = kn.Pool().Exec(context.Background(), `INSERT INTO parents(name) VALUES ($1)`, "p1")
	_, _ = kn.Pool().Exec(context.Background(), `INSERT INTO childs(parent_id) VALUES ($1)`, 1)
	_, _ = kn.Pool().Exec(context.Background(), `DELETE FROM parents WHERE id=$1`, 1)
}
