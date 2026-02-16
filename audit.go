package norm

import "context"

// AuditAction represents the type of operation being audited.
type AuditAction string

const (
	AuditActionCreate     AuditAction = "create"
	AuditActionUpdate     AuditAction = "update"
	AuditActionDelete     AuditAction = "delete"
	AuditActionSoftDelete AuditAction = "soft_delete"
	AuditActionRestore    AuditAction = "restore"
	AuditActionPurge      AuditAction = "purge"
	AuditActionUpsert     AuditAction = "upsert"
)

// AuditEntry contains metadata about a database operation for audit logging.
type AuditEntry struct {
	Action   AuditAction
	Table    string
	EntityID any    // primary key value (nil for bulk operations)
	Entity   any    // the entity pointer (nil for delete by id)
	Query    string // the SQL query executed
	Err      error  // non-nil if the operation failed
}

// AuditHook is a global hook interface for audit logging.
// Implement this and register via WithAuditHook option to receive
// notifications for all CRUD operations.
type AuditHook interface {
	// OnAudit is called after each auditable operation completes.
	// Implementations should be non-blocking and safe for concurrent use.
	OnAudit(ctx context.Context, entry AuditEntry)
}

// AuditHookFunc is a convenience adapter to use ordinary functions as AuditHook.
type AuditHookFunc func(ctx context.Context, entry AuditEntry)

func (f AuditHookFunc) OnAudit(ctx context.Context, entry AuditEntry) { f(ctx, entry) }
