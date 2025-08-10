package norm

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type okRows struct{}

func (o okRows) Values() ([]any, error)                       { return nil, nil }
func (o okRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (o okRows) Next() bool                                   { return false }
func (o okRows) Err() error                                   { return nil }
func (o okRows) Close()                                       {}
func (o okRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (o okRows) RawValues() [][]byte                          { return nil }
func (o okRows) Conn() *pgx.Conn                              { return nil }
func (o okRows) Scan(_ ...any) error                          { return nil }

type okExec struct{}

func (okExec) Exec(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}
func (okExec) Query(_ context.Context, _ string, _ ...any) (pgx.Rows, error) { return okRows{}, nil }
func (okExec) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row        { return okRows{} }

func TestBreakerExecuter_QueryAndRow(t *testing.T) {
	kn := &KintsNorm{}
	kn.breaker = newCircuitBreaker(circuitBreakerConfig{failureThreshold: 2})
	be := breakerExecuter{kn: kn, exec: okExec{}}
	if _, err := be.Query(context.Background(), "select 1"); err != nil {
		t.Fatalf("query: %v", err)
	}
	if err := be.QueryRow(context.Background(), "select 1").Scan(); err != nil {
		t.Fatalf("row scan: %v", err)
	}
}
