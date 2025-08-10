package norm

import "context"

// BeforeCreate can be implemented by a model to run logic before insert
type BeforeCreate interface {
	BeforeCreate(ctx context.Context) error
}

// AfterCreate can be implemented by a model to run logic after insert
type AfterCreate interface {
	AfterCreate(ctx context.Context) error
}

// BeforeUpdate can be implemented by a model to run logic before update
type BeforeUpdate interface {
	BeforeUpdate(ctx context.Context) error
}

// AfterUpdate can be implemented by a model to run logic after update
type AfterUpdate interface {
	AfterUpdate(ctx context.Context) error
}

// BeforeUpsert can be implemented by a model to run logic before upsert
type BeforeUpsert interface {
	BeforeUpsert(ctx context.Context) error
}

// AfterUpsert can be implemented by a model to run logic after upsert
type AfterUpsert interface {
	AfterUpsert(ctx context.Context) error
}

// Delete hooks
type BeforeDelete interface {
	BeforeDelete(ctx context.Context, id any) error
}
type AfterDelete interface {
	AfterDelete(ctx context.Context, id any) error
}

// SoftDelete hooks
type BeforeSoftDelete interface {
	BeforeSoftDelete(ctx context.Context, id any) error
}
type AfterSoftDelete interface {
	AfterSoftDelete(ctx context.Context, id any) error
}

// Restore hooks
type BeforeRestore interface {
	BeforeRestore(ctx context.Context, id any) error
}
type AfterRestore interface {
	AfterRestore(ctx context.Context, id any) error
}

// PurgeTrashed hooks
type BeforePurgeTrashed interface {
	BeforePurgeTrashed(ctx context.Context) error
}
type AfterPurgeTrashed interface {
	AfterPurgeTrashed(ctx context.Context, affected int64) error
}
