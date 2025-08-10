package norm

import (
	"reflect"
	"testing"
	"time"
)

type repoUser struct {
	ID        int64     `db:"id" orm:"primary_key,auto_increment"`
	Email     string    `db:"email"`
	UpdatedAt time.Time `db:"updated_at" orm:"on_update:now()"`
}

func TestRepo_TableName(t *testing.T) {
	r := &repo[repoUser]{}
	if r.tableName() != "repo_users" {
		t.Fatalf("table name: %s", r.tableName())
	}
}

func TestRepo_OnUpdateNowColumns(t *testing.T) {
	r := &repo[repoUser]{}
	cols := r.onUpdateNowColumns(reflect.TypeOf(repoUser{}))
	if !cols["updated_at"] {
		t.Fatalf("expected updated_at")
	}
}

func TestRepo_ExtractValuesByColumns(t *testing.T) {
	r := &repo[repoUser]{}
	u := &repoUser{ID: 1, Email: "a"}
	vals, err := r.extractValuesByColumns(u, []string{"id", "email"})
	if err != nil || len(vals) != 2 || vals[0].(int64) != 1 || vals[1].(string) != "a" {
		t.Fatalf("vals=%v err=%v", vals, err)
	}
	if _, err := r.extractValuesByColumns(u, []string{"unknown"}); err == nil {
		t.Fatalf("expected error for unknown column")
	}
}
