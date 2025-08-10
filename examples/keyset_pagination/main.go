package main

import (
	"context"
	"fmt"
	"log"

	"github.com/kintsdev/norm"
)

type KUser struct {
	ID       int64  `db:"id" norm:"primary_key,auto_increment"`
	Email    string `db:"email"`
	Username string `db:"username"`
}

func main() {
	cfg := &norm.Config{Host: "127.0.0.1", Port: 5432, Database: "postgres", Username: "postgres", Password: "postgres", SSLMode: "disable"}
	kn, err := norm.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer kn.Close()

	_ = kn.AutoMigrate(&KUser{})
	// seed a few
	for i := 0; i < 5; i++ {
		_, _ = kn.Pool().Exec(context.Background(), `INSERT INTO kusers(email, username) VALUES ($1,$2)`, fmt.Sprintf("k%02d@example.com", i), fmt.Sprintf("k%02d", i))
	}

	var page []KUser
	if err := kn.Query().Table("kusers").OrderBy("id ASC").After("id", 2).Limit(2).Find(context.Background(), &page); err != nil {
		log.Fatal(err)
	}
	for _, u := range page {
		fmt.Println(u.ID, u.Username)
	}
}
