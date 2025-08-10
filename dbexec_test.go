package norm

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// adapters to our executer wrappers to avoid real DB
type fakePgxExec struct{ err error }

func (f fakePgxExec) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, f.err
}
func (f fakePgxExec) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return nil, f.err
}
func (f fakePgxExec) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return fakeRowErr{err: f.err}
}

type fakeRowErr struct{ err error }

func (r fakeRowErr) Scan(dest ...any) error { return r.err }

func TestBreakerExecuter_ErrShortCircuit(t *testing.T) {
	kn := &KintsNorm{}
	kn.breaker = newCircuitBreaker(circuitBreakerConfig{failureThreshold: 1, openTimeout: time.Hour})
	// open it
	_ = kn.breaker.before()
	kn.breaker.after(errors.New("x"))
	be := breakerExecuter{kn: kn, exec: fakePgxExec{}}
	if _, err := be.Exec(context.Background(), "select 1"); err == nil {
		t.Fatalf("expected open err")
	}
}
