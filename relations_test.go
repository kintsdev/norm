package norm

import (
	"testing"
)

type relParent struct {
	ID int64 `db:"id"`
}
type relChild struct {
	ID     int64 `db:"id"`
	UserID int64 `db:"user_id"`
}

func TestLazyLoadMany(t *testing.T) {
	t.Skip("integration path depends on real pool; covered via e2e tests")
}
