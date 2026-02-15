package norm

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestWrapPgError_PassThroughAndCircuitOpen(t *testing.T) {
	// pass-through non-pg error
	e := errors.New("x")
	if wrapPgError(e, "q", nil) != e {
		t.Fatalf("expected passthrough")
	}
	// circuit open
	ce := circuitOpenErr
	out := wrapPgError(ce, "q", nil)
	oe, ok := out.(*ORMError)
	if !ok || oe.Code != ErrCodeConnection {
		t.Fatalf("expected connection code, got %#v", out)
	}
}

func TestMapPgErrorCode(t *testing.T) {
	cases := map[string]ErrorCode{
		"23505": ErrCodeDuplicate,
		"23503": ErrCodeConstraint,
		"23514": ErrCodeConstraint,
		"23502": ErrCodeConstraint,
		"40001": ErrCodeTransaction,
		"xxxxx": ErrCodeValidation,
	}
	for k, want := range cases {
		if got := mapPgErrorCode(k); got != want {
			t.Fatalf("code %s -> %v (got %v)", k, want, got)
		}
	}
}

func TestWrapPgError_ContextCanceled(t *testing.T) {
	out := wrapPgError(context.Canceled, "q", nil)
	oe, ok := out.(*ORMError)
	if !ok || oe.Code != ErrCodeTransaction {
		t.Fatalf("expected transaction code for context canceled, got %#v", out)
	}
	// also test context.DeadlineExceeded
	out2 := wrapPgError(context.DeadlineExceeded, "q2", nil)
	oe2, ok2 := out2.(*ORMError)
	if !ok2 || oe2.Code != ErrCodeTransaction {
		t.Fatalf("expected transaction code for deadline exceeded, got %#v", out2)
	}
}

func TestWrapPgError_PgErrMapping(t *testing.T) {
	// construct a pg error to hit the errors.As branch
	pgErr := &pgconn.PgError{Code: "23505", Message: "dup"}
	out := wrapPgError(pgErr, "q", []any{"a"})
	oe, ok := out.(*ORMError)
	if !ok || oe.Code != ErrCodeDuplicate || oe.Message == "" || len(oe.Args) != 1 || oe.Query != "q" {
		t.Fatalf("unexpected wrap: %#v", out)
	}
}

func TestORMError_Error(t *testing.T) {
	e := &ORMError{Code: ErrCodeValidation, Message: "m"}
	if e.Error() != "m" {
		t.Fatalf("error() not matching message")
	}
}

func TestORMError_Unwrap(t *testing.T) {
	inner := errors.New("inner cause")
	e := &ORMError{Code: ErrCodeConnection, Message: "wrapped", Internal: inner}
	if !errors.Is(e, inner) {
		t.Fatalf("Unwrap should allow errors.Is to find the internal error")
	}
	// nil internal should not panic
	e2 := &ORMError{Code: ErrCodeValidation, Message: "no internal"}
	if e2.Unwrap() != nil {
		t.Fatalf("Unwrap of nil internal should return nil")
	}
}
