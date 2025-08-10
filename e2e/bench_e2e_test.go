package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	kintsnorm "github.com/kintsdev/norm"
)

// Benchmarks below reuse global kn from TestMain in e2e_test.go

func BenchmarkE2E_InsertUsers(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, _ = kn.Pool().Exec(ctx, "TRUNCATE users RESTART IDENTITY CASCADE")
	repo := kintsnorm.NewRepository[User](kn)
	// warm-up
	_ = repo.Create(ctx, &User{Email: "warm@example.com", Username: "warm", Password: "x"})
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = repo.Create(ctx, &User{Email: fmt.Sprintf("b%08d@example.com", i), Username: fmt.Sprintf("b%08d", i), Password: "x"})
	}
}

func BenchmarkE2E_FindPage(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, _ = kn.Pool().Exec(ctx, "TRUNCATE users RESTART IDENTITY CASCADE")
	repo := kintsnorm.NewRepository[User](kn)
	// seed 200 users
	for i := 0; i < 200; i++ {
		_ = repo.Create(ctx, &User{Email: fmt.Sprintf("p%04d@example.com", i), Username: fmt.Sprintf("p%04d", i), Password: "x", IsActive: i%2 == 0})
	}
	pr := kintsnorm.PageRequest{Limit: 25, Offset: 50, OrderBy: "id DESC"}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = repo.FindPage(ctx, pr)
	}
}

func BenchmarkE2E_QueryBuilderScanStructs(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	// ensure some rows exist
	var cnt int
	_ = kn.Pool().QueryRow(ctx, "select count(*) from users").Scan(&cnt)
	if cnt < 1000 {
		repo := kintsnorm.NewRepository[User](kn)
		for i := 0; i < 1000-cnt; i++ {
			_ = repo.Create(ctx, &User{Email: fmt.Sprintf("s%06d@example.com", i), Username: fmt.Sprintf("s%06d", i), Password: "x"})
		}
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var users []User
		_ = kn.Query().Table("users").OrderBy("id DESC").Limit(100).Find(ctx, &users)
	}
}

func BenchmarkE2E_CopyFrom(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	_, _ = kn.Pool().Exec(ctx, "TRUNCATE users RESTART IDENTITY CASCADE")
	repo := kintsnorm.NewRepository[User](kn)
	// prepare batch entities of fixed size; body reused each iter
	batchSize := 500
	mkBatch := func(start int) []*User {
		out := make([]*User, 0, batchSize)
		for i := 0; i < batchSize; i++ {
			idx := start + i
			out = append(out, &User{Email: fmt.Sprintf("cf%08d@example.com", idx), Username: fmt.Sprintf("cf%08d", idx), Password: "pw"})
		}
		return out
	}
	// warm-up
	_ = repo.Create(ctx, &User{Email: "cf-warm@example.com", Username: "cf-warm", Password: "pw"})
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		batch := mkBatch(i * batchSize)
		_, _ = repo.CreateCopyFrom(ctx, batch, "email", "username", "password")
	}
}

func BenchmarkE2E_Upsert(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, _ = kn.Pool().Exec(ctx, "TRUNCATE users RESTART IDENTITY CASCADE")
	repo := kintsnorm.NewRepository[User](kn)
	// seed one
	_ = repo.Create(ctx, &User{Email: "up@example.com", Username: "up", Password: "x"})
	u := &User{Email: "up@example.com", Username: "up2", Password: "y"}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = repo.Upsert(ctx, u, []string{"email"}, []string{"username", "password"})
	}
}

func BenchmarkE2E_UpdatePartial(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, _ = kn.Pool().Exec(ctx, "TRUNCATE users RESTART IDENTITY CASCADE")
	repo := kintsnorm.NewRepository[User](kn)
	_ = repo.Create(ctx, &User{Email: "pp@example.com", Username: "pp", Password: "x"})
	// fetch id
	got, _ := repo.FindOne(ctx, kintsnorm.Eq("email", "pp@example.com"))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = repo.UpdatePartial(ctx, got.ID, map[string]any{"username": fmt.Sprintf("pp%04d", i)})
	}
}

func BenchmarkE2E_TxCommit(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, _ = kn.Pool().Exec(ctx, "TRUNCATE users RESTART IDENTITY CASCADE")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = kn.Tx().WithTransaction(ctx, func(tx kintsnorm.Transaction) error {
			r := kintsnorm.NewRepositoryWithExecutor[User](kn, tx.Exec())
			return r.Create(ctx, &User{Email: fmt.Sprintf("tx%08d@example.com", i), Username: fmt.Sprintf("tx%08d", i), Password: "pw"})
		})
	}
}

func BenchmarkE2E_RawQueries(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	_, _ = kn.Pool().Exec(ctx, "TRUNCATE calc_test")
	_ = kn.Query().Raw("CREATE TABLE IF NOT EXISTS calc_test(a int, b int)").Exec(ctx)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = kn.Query().Raw("INSERT INTO calc_test(a,b) VALUES(?,?)", 7, 5).Exec(ctx)
		var res []map[string]any
		_ = kn.Query().Raw("SELECT a + b AS s FROM calc_test").Find(ctx, &res)
	}
}
