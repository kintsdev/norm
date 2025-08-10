package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/kintsdev/norm"
)

// Money demonstrates type override with decimal(20,8)
type Money struct {
	ID        int64      `db:"id" norm:"primary_key,auto_increment"`
	Amount    float64    `db:"amount" norm:"type:decimal(20,8)"`
	CreatedAt time.Time  `db:"created_at" norm:"not_null,default:now()"`
	UpdatedAt time.Time  `db:"updated_at" norm:"not_null,default:now(),on_update:now()"`
	DeletedAt *time.Time `db:"deleted_at" norm:"index"`
}

func main() {
	cfg := &norm.Config{Host: "127.0.0.1", Port: 5432, Database: "postgres", Username: "postgres", Password: "postgres", SSLMode: "disable"}
	kn, err := norm.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer kn.Close()

	if err := kn.AutoMigrate(&Money{}); err != nil {
		log.Fatal(err)
	}

	// Insert with builder
	if _, err := kn.Query().Table("moneys").Insert("amount").Values(123123.45678901).ExecInsert(context.Background(), nil); err != nil {
		log.Fatal("insert", err)
	}

	var m Money
	if err := kn.Query().Table("moneys").Where("id = ?", 1).First(context.Background(), &m); err != nil {
		log.Fatal("first ", err)
	}
	fmt.Println(m)

	// Update with builder
	if _, err := kn.Query().Table("moneys").Where("id = ?", 1).Set("amount = ?", 123123.45678901).ExecUpdate(context.Background(), nil); err != nil {
		log.Fatal("update", err)
	}

	// soft delete
	count, err := kn.Query().Table("moneys").Where("id = ?", 2).Delete(context.Background())
	if err != nil {
		log.Fatal("delete", err)
	}
	fmt.Println(count)

	// hard delete last
	var last Money
	if err := kn.Query().Table("moneys").OrderBy("created_at DESC").Last(context.Background(), &last); err != nil {
		log.Fatal("last", err)
	}
	fmt.Println("last", last)

	// hard delete
	count, err = kn.Query().Table("moneys").Where("id = ?", last.ID).HardDelete().Delete(context.Background())
	if err != nil {
		log.Fatal("hard delete", err)
	}
	fmt.Println(count)

}
