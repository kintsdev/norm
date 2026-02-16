package norm

import (
	"context"
	"fmt"
	"strings"
)

// SetSessionVar sets a PostgreSQL session variable (e.g., `SET app.current_user = 'user123'`).
// Useful for Row-Level Security (RLS) policies that reference session variables.
// The value is properly quoted to prevent injection.
func (kn *KintsNorm) SetSessionVar(ctx context.Context, key, value string) error {
	query := fmt.Sprintf("SET %s = %s", quoteSessionKey(key), quoteSessionValue(value))
	_, err := kn.pool.Exec(ctx, query)
	if err != nil {
		return &ORMError{Code: ErrCodeInternal, Message: fmt.Sprintf("set session var %s: %s", key, err.Error()), Internal: err}
	}
	return nil
}

// ResetSessionVar resets a session variable to its default value.
func (kn *KintsNorm) ResetSessionVar(ctx context.Context, key string) error {
	query := fmt.Sprintf("RESET %s", quoteSessionKey(key))
	_, err := kn.pool.Exec(ctx, query)
	if err != nil {
		return &ORMError{Code: ErrCodeInternal, Message: fmt.Sprintf("reset session var %s: %s", key, err.Error()), Internal: err}
	}
	return nil
}

// SetRole executes `SET ROLE <role>` to switch the current session role.
// Useful for RLS enforcement where queries should run as a specific database role.
func (kn *KintsNorm) SetRole(ctx context.Context, role string) error {
	query := fmt.Sprintf("SET ROLE %s", quoteSessionValue(role))
	_, err := kn.pool.Exec(ctx, query)
	if err != nil {
		return &ORMError{Code: ErrCodeInternal, Message: fmt.Sprintf("set role: %s", err.Error()), Internal: err}
	}
	return nil
}

// ResetRole resets the session role to the default (connection user).
func (kn *KintsNorm) ResetRole(ctx context.Context) error {
	_, err := kn.pool.Exec(ctx, "RESET ROLE")
	if err != nil {
		return &ORMError{Code: ErrCodeInternal, Message: fmt.Sprintf("reset role: %s", err.Error()), Internal: err}
	}
	return nil
}

// RLSContext represents RLS session configuration to be applied within a transaction.
type RLSContext struct {
	Role        string            // role to SET ROLE to (empty means no role change)
	SessionVars map[string]string // session variables to SET (e.g., "app.current_user" -> "user123")
}

// WithRLS executes a function within a transaction with RLS session variables and role set.
// All session vars and role are set at the beginning, and the transaction is committed or
// rolled back at the end. This ensures RLS policies see the correct context.
func (kn *KintsNorm) WithRLS(ctx context.Context, rls RLSContext, fn func(tx Transaction) error) error {
	txx, err := kn.Tx().BeginTx(ctx, &TxOptions{})
	if err != nil {
		return err
	}
	exec := txx.Exec()

	// Set role if specified
	if rls.Role != "" {
		query := fmt.Sprintf("SET LOCAL ROLE %s", quoteSessionValue(rls.Role))
		if _, err := exec.Exec(ctx, query); err != nil {
			_ = txx.Rollback(ctx)
			return &ORMError{Code: ErrCodeInternal, Message: fmt.Sprintf("set role in tx: %s", err.Error()), Internal: err}
		}
	}

	// Set session variables (using SET LOCAL so they're scoped to the transaction)
	for key, value := range rls.SessionVars {
		query := fmt.Sprintf("SET LOCAL %s = %s", quoteSessionKey(key), quoteSessionValue(value))
		if _, err := exec.Exec(ctx, query); err != nil {
			_ = txx.Rollback(ctx)
			return &ORMError{Code: ErrCodeInternal, Message: fmt.Sprintf("set session var %s in tx: %s", key, err.Error()), Internal: err}
		}
	}

	// Execute user function
	if err := fn(txx); err != nil {
		_ = txx.Rollback(ctx)
		return err
	}

	return txx.Commit(ctx)
}

// quoteSessionKey validates and returns a safe session key (dotted identifier like app.current_user)
func quoteSessionKey(key string) string {
	// Session variable keys are dotted identifiers; quote each part
	parts := strings.Split(key, ".")
	for i, p := range parts {
		parts[i] = QuoteIdentifier(strings.TrimSpace(p))
	}
	return strings.Join(parts, ".")
}

// quoteSessionValue safely quotes a value for SET commands
func quoteSessionValue(value string) string {
	// Use single-quote escaping (double any embedded single quotes)
	escaped := strings.ReplaceAll(value, "'", "''")
	return "'" + escaped + "'"
}
