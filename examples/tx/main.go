package main

import (
	"context"
	"log"

	"github.com/kintsdev/norm"
)

type TUser struct {
	ID    int64  `db:"id" norm:"primary_key,auto_increment"`
	Email string `db:"email"`
}

func main() {
	cfg := &norm.Config{Host: "127.0.0.1", Port: 5432, Database: "postgres", Username: "postgres", Password: "postgres", SSLMode: "disable"}
	kn, err := norm.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer kn.Close()
	if err := kn.AutoMigrate(&TUser{}); err != nil {
		log.Fatal(err)
	}

	// rollback example
	_ = kn.Tx().WithTransaction(context.Background(), func(tx norm.Transaction) error {
		if _, err := tx.Exec().Exec(context.Background(), `INSERT INTO t_users(email) VALUES ($1)`, "tx@example.com"); err != nil {
			return err
		}
		return &norm.ORMError{Code: norm.ErrCodeTransaction, Message: "force rollback"}
	})

	// commit
	if err := kn.Tx().WithTransaction(context.Background(), func(tx norm.Transaction) error {
		if _, err := tx.Exec().Exec(context.Background(), `INSERT INTO t_users(email) VALUES ($1)`, "tx2@example.com"); err != nil {
			return err
		}
		return nil
	}); err != nil {
		log.Fatal(err)
	}
}
