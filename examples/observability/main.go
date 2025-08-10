package main

import (
	"context"
	"expvar"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/kintsdev/norm"
)

func main() {
	ctx := context.Background()
	cfg := &norm.Config{
		Host:                   "localhost",
		Port:                   5432,
		Database:               "postgres",
		Username:               "postgres",
		Password:               "postgres",
		MaxConnections:         5,
		MinConnections:         1,
		StatementCacheCapacity: 256,
	}

	db, err := norm.New(
		cfg,
		norm.WithLogger(norm.StdLogger{}),
		norm.WithLogMode(norm.LogInfo),
		// Add correlation/request IDs and other context fields to every log line
		norm.WithLogContextFields(func(ctx context.Context) []norm.Field {
			if v := ctx.Value("corr_id"); v != nil {
				return []norm.Field{{Key: "corr_id", Value: v}}
			}
			return nil
		}),
		// Mark queries slower than 100ms as slow_query
		norm.WithSlowQueryThreshold(100*time.Millisecond),
		// Hide SQL parameters in logs; avoids inline stmt construction
		norm.WithLogParameterMasking(true),
		// Simple stdlib metrics adapter; check /debug/vars
		norm.WithMetrics(norm.ExpvarMetrics{}),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if err := db.Health(ctx); err != nil {
		log.Fatal("health:", err)
	}

	// Correlation ID injected via context; appears in logs
	ctx = context.WithValue(ctx, "corr_id", "demo-123")

	// Fast query: params are masked; no inline stmt field generated
	var rows []map[string]any
	if err := db.Query().Raw("SELECT 1 WHERE $1::int = $2::int", 1, 1).Find(ctx, &rows); err != nil {
		log.Fatal("fast query:", err)
	}
	fmt.Println("fast query ok; preparing metrics endpoint...")

	// Slow query to trigger slow_query logging
	_ = db.Query().Raw("SELECT pg_sleep(0.2)").Find(ctx, &rows)
	fmt.Println("slow query executed (should log warn: slow_query)")

	// Start HTTP server AFTER metrics have been touched, and keep process alive until Ctrl+C
	mux := http.NewServeMux()
	mux.Handle("/debug/vars", expvar.Handler())
	srv := &http.Server{Addr: ":8080", Handler: mux}
	go func() {
		log.Printf("metrics available at http://localhost:8080/debug/vars")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("http server: %v", err)
		}
	}()

	// Wait for interrupt to exit
	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-sigCtx.Done()
	_ = srv.Shutdown(context.Background())
}
