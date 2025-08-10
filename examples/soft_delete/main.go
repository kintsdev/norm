package main

import (
	"context"
	"log"
	"time"

	"github.com/kintsdev/norm"
)

// SDUser demonstrates soft delete with DeletedAt column
type SDUser struct {
	ID        int64      `db:"id" norm:"primary_key,auto_increment"`
	Email     string     `db:"email" norm:"unique,not_null"`
	Username  string     `db:"username"`
	DeletedAt *time.Time `db:"deleted_at"`
}

func main() {
	cfg := &norm.Config{Host: "127.0.0.1", Port: 5432, Database: "postgres", Username: "postgres", Password: "postgres", SSLMode: "disable"}
	kn, err := norm.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer kn.Close()

	if err := kn.AutoMigrate(&SDUser{}); err != nil {
		log.Fatal(err)
	}

	repo := norm.NewRepository[SDUser](kn)
	if err := repo.Create(context.Background(), &SDUser{Email: "sd@example.com", Username: "sd"}); err != nil {
		log.Fatal(err)
	}
	u, err := repo.FindOne(context.Background(), norm.Eq("email", "sd@example.com"))
	if err != nil {
		log.Fatal(err)
	}

	// soft delete
	if err := repo.SoftDelete(context.Background(), u.ID); err != nil {
		log.Fatal(err)
	}

	// OnlyTrashed should find it
	if _, err := repo.OnlyTrashed().FindOne(context.Background(), norm.Eq("id", u.ID)); err != nil {
		log.Fatal("expected trashed row to be found")
	}
}
