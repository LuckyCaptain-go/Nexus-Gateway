package olap

import (
	"fmt"
	"time"
)

// ClickHouseConfig holds ClickHouse connection configuration
type ClickHouseConfig struct {
	Host            string
	Port            int
	Database        string
	Username        string
	Password        string
	Compress        bool    // Enable data compression
	Protocol        string  // "native" or "http"
	Timeout         time.Duration
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	SSLEnabled      bool
	SSLVerify       bool
	OpenTelemetry   bool
}

// NewClickHouseConfig creates a new ClickHouse configuration
func NewClickHouseConfig() *ClickHouseConfig {
	return &ClickHouseConfig{
		Host:            "localhost",
		Port:            9000,
		Database:        "default",
		Username:        "default",
		Password:        "",
		Compress:        true,
		Protocol:        "native",
		Timeout:         30 * time.Second,
		ReadTimeout:     30 * time.Second,
		WriteTimeout:    30 * time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: 1 * time.Hour,
		SSLEnabled:      false,
		SSLVerify:       true,
		OpenTelemetry:   false,
	}
}

// Validate validates the configuration
func (c *ClickHouseConfig) Validate() error {
	if c.Host == "" {
		return fmt.Errorf("host is required")
	}
	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("port must be between 1 and 65535")
	}
	if c.Database == "" {
		return fmt.Errorf("database is required")
	}
	if c.Username == "" {
		return fmt.Errorf("username is required")
	}
	if c.Protocol != "native" && c.Protocol != "http" {
		return fmt.Errorf("protocol must be 'native' or 'http'")
	}
	return nil
}

// GetHTTPPort returns the HTTP port (native port + 1)
func (c *ClickHouseConfig) GetHTTPPort() int {
	return c.Port + 1
}

// GetDSN builds the ClickHouse DSN string
func (c *ClickHouseConfig) GetDSN() string {
	if c.Protocol == "http" {
		return fmt.Sprintf("http://%s:%s@%s:%d/%s",
			c.Username, c.Password, c.Host, c.GetHTTPPort(), c.Database)
	}
	return fmt.Sprintf("tcp://%s:%s@%s:%d/%s",
		c.Username, c.Password, c.Host, c.Port, c.Database)
}

// IsCompressionEnabled returns whether compression is enabled
func (c *ClickHouseConfig) IsCompressionEnabled() bool {
	return c.Compress
}

// GetConnectionString returns the full connection string
func (c *ClickHouseConfig) GetConnectionString() string {
	dsn := c.GetDSN()

	params := []string{}
	if c.Compress {
		params = append(params, "compress=true")
	}
	if c.SSLEnabled {
		params = append(params, "secure=true")
	}

	if len(params) > 0 {
		dsn += "?" + params[0]
		for i := 1; i < len(params); i++ {
			dsn += "&" + params[i]
		}
	}

	return dsn
}

// SetDefaults sets default values for empty fields
func (c *ClickHouseConfig) SetDefaults() {
	if c.Host == "" {
		c.Host = "localhost"
	}
	if c.Port == 0 {
		c.Port = 9000
	}
	if c.Database == "" {
		c.Database = "default"
	}
	if c.Username == "" {
		c.Username = "default"
	}
	if c.Timeout == 0 {
		c.Timeout = 30 * time.Second
	}
	if c.ReadTimeout == 0 {
		c.ReadTimeout = 30 * time.Second
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = 30 * time.Second
	}
	if c.MaxOpenConns == 0 {
		c.MaxOpenConns = 10
	}
	if c.MaxIdleConns == 0 {
		c.MaxIdleConns = 5
	}
	if c.ConnMaxLifetime == 0 {
		c.ConnMaxLifetime = 1 * time.Hour
	}
}
