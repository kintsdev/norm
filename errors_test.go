package norm

import (
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
	out := wrapPgError(contextCanceledErr(), "q", nil)
	oe, ok := out.(*ORMError)
	if !ok || oe.Code != ErrCodeTransaction {
		t.Fatalf("expected transaction code for context canceled, got %#v", out)
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
