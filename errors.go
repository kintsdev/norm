package norm

import (
	"errors"

	"github.com/jackc/pgx/v5/pgconn"
)

type ErrorCode int

const (
	ErrCodeConnection ErrorCode = iota
	ErrCodeNotFound
	ErrCodeDuplicate
	ErrCodeConstraint
	ErrCodeTransaction
	ErrCodeMigration
	ErrCodeValidation
)

// ORMError is a structured error for norm
type ORMError struct {
	Code     ErrorCode
	Message  string
	Internal error
	Query    string
	Args     []interface{}
}

func (e *ORMError) Error() string { return e.Message }

// pg error mapping: map common PostgreSQL errors to ORMError codes

func mapPgErrorCode(pgCode string) ErrorCode {
	switch pgCode {
	case "23505": // unique_violation
		return ErrCodeDuplicate
	case "23503": // foreign_key_violation
		return ErrCodeConstraint
	case "23514": // check_violation
		return ErrCodeConstraint
	case "23502": // not_null_violation
		return ErrCodeConstraint
	case "40001": // serialization_failure
		return ErrCodeTransaction
	default:
		return ErrCodeValidation
	}
}

func wrapPgError(err error, query string, args []any) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.Is(err, contextCanceledErr()) {
		return &ORMError{Code: ErrCodeTransaction, Message: err.Error(), Internal: err, Query: query, Args: args}
	}
	if errors.As(err, &pgErr) {
		return &ORMError{Code: mapPgErrorCode(pgErr.Code), Message: pgErr.Message, Internal: err, Query: query, Args: args}
	}
	return err
}

// avoid importing context; detect cancellation by error string (best-effort)
func contextCanceledErr() error { return errors.New("context canceled") }
