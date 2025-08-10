package norm

import (
	"strings"
	"testing"
	"time"
)

func TestConnString_Defaults(t *testing.T) {
	c := &Config{Database: "postgres", Username: "u", Password: "p", ApplicationName: "app"}
	s := c.ConnString()
	if !strings.Contains(s, "host=localhost") || !strings.Contains(s, "port=5432") || !strings.Contains(s, "sslmode=disable") {
		t.Fatalf("defaults missing: %s", s)
	}
}

func TestConnString_Custom(t *testing.T) {
	c := &Config{Host: "h", Port: 5555, SSLMode: "require", Database: "d", Username: "u", Password: "p", ApplicationName: "app", ConnectTimeout: 3 * time.Second}
	s := c.ConnString()
	if !strings.Contains(s, "host=h") || !strings.Contains(s, "port=5555") || !strings.Contains(s, "sslmode=require") || !strings.Contains(s, "connect_timeout=3") {
		t.Fatalf("custom mismatch: %s", s)
	}
}
