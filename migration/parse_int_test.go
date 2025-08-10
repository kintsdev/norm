package migration

import "testing"

func TestParseInt64(t *testing.T) {
	n, err := parseInt64("12345")
	if err != nil || n != 12345 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	if _, err := parseInt64("12a"); err == nil {
		t.Fatalf("expected error")
	}
}
