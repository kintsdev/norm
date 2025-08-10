package norm

import (
	"fmt"
	"time"
)

// Config holds database and runtime configuration for Kints-Norm
type Config struct {
	Host                   string
	Port                   int
	Database               string
	Username               string
	Password               string
	SSLMode                string
	MaxConnections         int32
	MinConnections         int32
	MaxConnLifetime        time.Duration
	MaxConnIdleTime        time.Duration
	HealthCheckPeriod      time.Duration
	ConnectTimeout         time.Duration
	ApplicationName        string
	ReadOnlyConnString     string        // optional DSN for read replica(s)
	RetryAttempts          int           // transient error retries (default 0 = no retry)
	RetryBackoff           time.Duration // backoff between retries
	StatementCacheCapacity int           // pgx per-conn statement cache capacity (0 = default)
}

// ConnString returns a PostgreSQL connection string compatible with pgx
func (c *Config) ConnString() string {
	ssl := c.SSLMode
	if ssl == "" {
		ssl = "disable"
	}
	host := c.Host
	if host == "" {
		host = "localhost"
	}
	port := c.Port
	if port == 0 {
		port = 5432
	}
	return fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=%s application_name=%s connect_timeout=%d",
		host,
		port,
		c.Database,
		c.Username,
		c.Password,
		ssl,
		c.ApplicationName,
		int(c.ConnectTimeout.Seconds()),
	)
}
