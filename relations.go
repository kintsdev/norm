package norm

import (
	"context"
	"fmt"
	"reflect"

	core "github.com/kintsdev/norm/internal/core"
)

// EagerLoadMany loads related rows of type R for the given parents of type T using childForeignKey.
// getParentID extracts the parent id used to match against childForeignKey.
// It groups children by childForeignKey matching each parent's id and invokes set(parent, children).
func EagerLoadMany[T any, R any](ctx context.Context, kn *KintsNorm, parents []*T, getParentID func(*T) any, childForeignKey string, set func(parent *T, children []*R)) error {
	if len(parents) == 0 {
		return nil
	}
	// Collect parent IDs as strings for grouping
	ids := make([]any, 0, len(parents))
	for _, p := range parents {
		idVal := getParentID(p)
		ids = append(ids, idVal)
	}
	// Query children by IN
	var rvar R
	rType := reflect.TypeOf(rvar)
	childTable := core.ToSnakeCase(rType.Name()) + "s"
	var children []R
	if err := kn.Query().Table(childTable).WhereNamed(childForeignKey+" IN :ids", map[string]any{"ids": ids}).Find(ctx, &children); err != nil {
		return err
	}
	// Group by child FK
	mapperC := core.StructMapper(rType)
	fiC, ok := mapperC.FieldsByColumn[childForeignKey]
	if !ok {
		return fmt.Errorf("child foreign key column not found in struct: %s", childForeignKey)
	}
	groups := make(map[string][]*R)
	for i := range children {
		rv := reflect.Indirect(reflect.ValueOf(children[i]))
		fk := fmt.Sprint(rv.FieldByIndex(fiC.Index).Interface())
		rptr := &children[i]
		groups[fk] = append(groups[fk], rptr)
	}
	// Assign back
	for _, p := range parents {
		id := fmt.Sprint(getParentID(p))
		set(p, groups[id])
	}
	return nil
}

// LazyLoadMany loads children by a single parent ID via childForeignKey
func LazyLoadMany[R any](ctx context.Context, kn *KintsNorm, parentID any, childForeignKey string) ([]*R, error) {
	var rvar R
	rType := reflect.TypeOf(rvar)
	childTable := core.ToSnakeCase(rType.Name()) + "s"
	var rows []R
	if err := kn.Query().Table(childTable).Where(childForeignKey+" = ?", parentID).Find(ctx, &rows); err != nil {
		return nil, err
	}
	out := make([]*R, 0, len(rows))
	for i := range rows {
		out = append(out, &rows[i])
	}
	return out, nil
}
