## Errors

Norm wraps common PostgreSQL errors into `ORMError` with a code and original error.

Codes:

- ErrCodeConnection
- ErrCodeNotFound
- ErrCodeDuplicate
- ErrCodeConstraint
- ErrCodeTransaction
- ErrCodeMigration
- ErrCodeValidation

On failures, `wrapPgError` attaches `Query` and `Args`. For context cancellation, a transaction error is returned. Circuit open is mapped to connection error.


