package norm

import (
	"testing"
)

func TestBuilderSettersAndNamed(t *testing.T) {
	kn := &KintsNorm{}
	qb := (&QueryBuilder{kn: kn}).TableQ("public.users").SelectQ("users.id", "users.name").SelectQI("na.me").
		Where("a = ?", 1).WhereNamed("b = :b AND c IN :c", map[string]any{"b": 2, "c": []int{3, 4}}).
		OrderBy("id ASC").Limit(5).Offset(10).WithCacheKey("k", 0).WithInvalidateKeys("i1", "i2")
	sql, args := qb.buildSelect()
	if sql == "" || len(args) == 0 {
		t.Fatalf("unexpected build")
	}

	// WhereCond
	qb2 := (&QueryBuilder{kn: kn}).Table("t").WhereCond(Eq("x", 1))
	if s, a := qb2.buildSelect(); s == "" || len(a) != 1 {
		t.Fatalf("wherecond")
	}

	// RawNamed success
	qb3 := (&QueryBuilder{kn: kn}).RawNamed("select * from t where id = :id", map[string]any{"id": 7})
	if qb3.raw == "" || !qb3.isRaw {
		t.Fatalf("rawn")
	}

	// UsePrimary / UseReadPool should not panic
	qb.UsePrimary()
	qb.UseReadPool()
}
