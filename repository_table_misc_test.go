package norm

import (
	"testing"
)

type simpleModel struct {
	ID   int64  `db:"id" norm:"primary_key,auto_increment"`
	Name string `db:"name"`
}

func TestRepo_TableNamePluralization(t *testing.T) {
	r := &repo[simpleModel]{}
	if r.tableName() != "simple_models" {
		t.Fatalf("table: %s", r.tableName())
	}
}
