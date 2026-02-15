package norm

import (
	"context"
	"errors"
	"fmt"

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
	// Specific validation subtypes
	ErrCodeInvalidColumn
	ErrCodeInvalidFunction
	ErrCodeInvalidCast
	ErrCodeStringTooLong
)

// ORMError is a structured error for norm
type ORMError struct {
	Code     ErrorCode
	Message  string
	Internal error
	Query    string
	Args     []any
}

func (e *ORMError) Error() string { return e.Message }

// Unwrap returns the internal error so errors.Is/errors.As can traverse the chain
func (e *ORMError) Unwrap() error { return e.Internal }

// pg error mapping: map common PostgreSQL errors to ORMError codes

func mapPgErrorCode(pgCode string) ErrorCode {
	switch pgCode {
	// duplicate / constraint family
	case "23505": // unique_violation
		return ErrCodeDuplicate
	case "23503": // foreign_key_violation
		return ErrCodeConstraint
	case "23514": // check_violation
		return ErrCodeConstraint
	case "23502": // not_null_violation
		return ErrCodeConstraint
	case "23513": // exclusion_violation
		return ErrCodeConstraint
	// transaction / concurrency
	case "40001": // serialization_failure
		return ErrCodeTransaction
	case "40P01": // deadlock_detected
		return ErrCodeTransaction
	case "55P03": // lock_not_available
		return ErrCodeTransaction
	case "57014": // query_canceled
		return ErrCodeTransaction
	// connection related
	case "08000", // connection_exception
		"08001", // sqlclient_unable_to_establish_sqlconnection
		"08003", // connection_does_not_exist
		"08004", // sqlserver_rejected_establishment_of_sqlconnection
		"08006", // connection_failure
		"57P01", // admin_shutdown
		"57P02", // crash_shutdown
		"57P03", // cannot_connect_now
		"53300": // too_many_connections
		return ErrCodeConnection
		// specific invalid column
	case "42703": // undefined_column
		return ErrCodeInvalidColumn
	// validation / syntax / undefined objects / cast issues
	case "42601", // syntax_error
		"42P01": // undefined_table (keep generic to avoid breaking callers/tests)
		return ErrCodeValidation
	case "42883": // undefined_function
		return ErrCodeInvalidFunction
	case "22P02": // invalid_text_representation
		return ErrCodeInvalidCast
	case "22001": // string_data_right_truncation
		return ErrCodeStringTooLong
	default:
		return ErrCodeValidation
	}
}

func wrapPgError(err error, query string, args []any) error {
	if err == nil {
		return nil
	}
	// If already wrapped, return as-is
	var oe *ORMError
	if errors.As(err, &oe) {
		return err
	}
	var pgErr *pgconn.PgError
	// detect context cancellation / deadline exceeded
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return &ORMError{Code: ErrCodeTransaction, Message: err.Error(), Internal: err, Query: query, Args: args}
	}
	// pass through circuit breaker open error as connection error with message
	if isCircuitOpenError(err) {
		return &ORMError{Code: ErrCodeConnection, Message: fmt.Sprintf("circuit open: %v", err), Internal: err, Query: query, Args: args}
	}
	if errors.As(err, &pgErr) {
		return &ORMError{Code: mapPgErrorCode(pgErr.Code), Message: pgErr.Message, Internal: err, Query: query, Args: args}
	}
	return err
}
