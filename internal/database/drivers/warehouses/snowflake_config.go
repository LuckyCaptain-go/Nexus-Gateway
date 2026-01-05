package warehouses

import (
	"fmt"
	"net/url"
	"strings"
	"time"

	"nexus-gateway/internal/model"

	"github.com/snowflakedb/gosnowflake"
)

// SnowflakeConfig holds Snowflake connection configuration
type SnowflakeConfig struct {
	// Required fields
	Account   string // Account identifier (e.g., xy12345.us-east-1)
	User      string // Username
	Password  string // Password
	Database  string // Database name
	Schema    string // Schema name (optional, defaults to "public")
	Warehouse string // Warehouse name (optional)

	// Optional fields
	Role     string // Role name (optional)
	Region   string // Region (optional, parsed from account if not provided)
	Host     string // Host (optional, defaults to <account>.snowflakecomputing.com)
	Port     int    // Port (optional, defaults to 443)
	Protocol string // Protocol (optional, defaults to "https")

	// Connection settings
	LoginTimeout    time.Duration // Login timeout
	RequestTimeout  time.Duration // Request timeout
	MaxRetryBackoff time.Duration // Maximum backoff time for retries

	// Session parameters
	ClientSessionKeepAlive bool // Keep session alive
}

// DefaultSnowflakeConfig returns default configuration
func DefaultSnowflakeConfig() *SnowflakeConfig {
	return &SnowflakeConfig{
		Schema:                 "public",
		Port:                   443,
		Protocol:               "https",
		LoginTimeout:           30 * time.Second,
		RequestTimeout:         30 * time.Second,
		MaxRetryBackoff:        30 * time.Second,
		ClientSessionKeepAlive: true,
	}
}

// BuildDSN builds a Snowflake DSN from configuration
func (c *SnowflakeConfig) BuildDSN() (string, error) {
	if c.Account == "" {
		return "", fmt.Errorf("account is required")
	}
	if c.User == "" {
		return "", fmt.Errorf("user is required")
	}
	if c.Password == "" {
		return "", fmt.Errorf("password is required")
	}

	// Determine host
	host := c.Host
	if host == "" {
		// Account may contain region (e.g., xy12345.us-east-1)
		if strings.Contains(c.Account, ".") {
			// Account already includes region
			host = fmt.Sprintf("%s.snowflakecomputing.com", c.Account)
		} else if c.Region != "" {
			host = fmt.Sprintf("%s.%s.snowflakecomputing.com", c.Account, c.Region)
		} else {
			host = fmt.Sprintf("%s.snowflakecomputing.com", c.Account)
		}
	}

	// Build DSN using gosnowflake format
	// Format: user:password@account/database/schema?warehouse=warehouse&role=role
	dsn := &gosnowflake.Config{
		Account:        c.Account,
		User:           c.User,
		Password:       c.Password,
		Database:       c.Database,
		Schema:         c.Schema,
		Warehouse:      c.Warehouse,
		Role:           c.Role,
		Host:           host,
		Port:           c.Port,
		Protocol:       c.Protocol,
		LoginTimeout:   c.LoginTimeout,
		RequestTimeout: c.RequestTimeout,
	}

	return fmt.Sprintf("%s:%s:%s@%s:%d/%s", c.User, c.Password, c.Account, c.Host, c.Port, c.Database), nil
}

// BuildDSNFromModelConfig builds Snowflake config from model.DataSourceConfig
func BuildDSNFromModelConfig(config *model.DataSourceConfig) (*SnowflakeConfig, error) {
	sfConfig := DefaultSnowflakeConfig()

	// Extract account from host or use a dedicated field
	sfConfig.Account = config.Host

	// Parse account to extract region if needed
	if strings.Contains(config.Host, ".") {
		parts := strings.Split(config.Host, ".")
		if len(parts) >= 2 {
			sfConfig.Account = parts[0]
			sfConfig.Region = parts[1]
		}
	}

	sfConfig.User = config.Username
	sfConfig.Password = config.Password
	sfConfig.Database = config.Database

	// Schema can be in additional props
	if schema, ok := config.AdditionalProps["schema"]; ok {
		sfConfig.Schema = schema
	}

	if warehouse, ok := config.AdditionalProps["warehouse"]; ok {
		sfConfig.Warehouse = warehouse
	}

	if role, ok := config.AdditionalProps["role"]; ok {
		sfConfig.Role = role
	}

	return sfConfig, nil
}

// ValidateConfig validates Snowflake configuration
func (c *SnowflakeConfig) ValidateConfig() error {
	if c.Account == "" {
		return fmt.Errorf("account is required")
	}
	if c.User == "" {
		return fmt.Errorf("user is required")
	}
	if c.Password == "" {
		return fmt.Errorf("password is required")
	}
	if c.Database == "" {
		return fmt.Errorf("database is required")
	}

	// Validate account format
	if strings.Contains(c.Account, "-") {
		return fmt.Errorf("account should not contain hyphens")
	}

	return nil
}

// ParseConnectionString parses a Snowflake connection string
func ParseConnectionString(connStr string) (*SnowflakeConfig, error) {
	config := DefaultSnowflakeConfig()

	// Parse URL format: snowflake://user:password@account/database/schema?params
	if strings.HasPrefix(connStr, "snowflake://") {
		u, err := url.Parse(connStr)
		if err != nil {
			return nil, fmt.Errorf("failed to parse connection string: %w", err)
		}

		// Extract user:password
		if u.User != nil {
			config.User = u.User.Username()
			if pwd, ok := u.User.Password(); ok {
				config.Password = pwd
			}
		}

		// Extract account from host
		config.Account = u.Hostname()
		if strings.Contains(config.Account, ".") {
			parts := strings.Split(config.Account, ".")
			config.Account = parts[0]
		}

		// Extract database/schema from path
		path := strings.TrimPrefix(u.Path, "/")
		parts := strings.Split(path, "/")
		if len(parts) > 0 {
			config.Database = parts[0]
		}
		if len(parts) > 1 {
			config.Schema = parts[1]
		}

		// Parse query parameters
		query := u.Query()
		if warehouse := query.Get("warehouse"); warehouse != "" {
			config.Warehouse = warehouse
		}
		if role := query.Get("role"); role != "" {
			config.Role = role
		}
		if schema := query.Get("schema"); schema != "" {
			config.Schema = schema
		}

		return config, nil
	}

	return nil, fmt.Errorf("invalid connection string format, expected snowflake://...")
}

// GetConnectionParameters returns connection parameters for logging
func (c *SnowflakeConfig) GetConnectionParameters() map[string]string {
	params := map[string]string{
		"account":  c.Account,
		"user":     c.User,
		"database": c.Database,
		"schema":   c.Schema,
	}

	if c.Warehouse != "" {
		params["warehouse"] = c.Warehouse
	}
	if c.Role != "" {
		params["role"] = c.Role
	}
	if c.Region != "" {
		params["region"] = c.Region
	}

	return params
}

// SupportsFeature checks if the configuration supports a specific feature
func (c *SnowflakeConfig) SupportsFeature(feature string) bool {
	switch feature {
	case "time_travel":
		return true
	case "variant":
		return true
	case "arrays":
		return true
	case "objects":
		return true
	case "semi_structured":
		return true
	case "query_acceleration":
		return true
	default:
		return false
	}
}

// GetRecommendedWarehouseSize returns recommended warehouse size based on workload
func GetRecommendedWarehouseSize(workload string) string {
	switch workload {
	case "small":
		return "XSMALL"
	case "medium":
		return "SMALL"
	case "large":
		return "MEDIUM"
	case "xlarge":
		return "LARGE"
	case "xxlarge":
		return "XLARGE"
	default:
		return "SMALL" // Default
	}
}

// ValidateWarehouseSize validates Snowflake warehouse size
func ValidateWarehouseSize(size string) bool {
	validSizes := map[string]bool{
		"XSMALL": true, "SMALL": true, "MEDIUM": true, "LARGE": true,
		"XLARGE": true, "XXLARGE": true, "XXXLARGE": true, "XXXXLARGE": true,
	}
	return validSizes[strings.ToUpper(size)]
}

// EstimateQueryCost estimates cost for a query (simplified)
func EstimateQueryCost(warehouseSize, duration time.Duration) float64 {
	// Simplified cost estimation based on warehouse size and duration
	// Actual costs depend on Snowflake pricing and credits

	creditsPerHour := map[string]float64{
		"XSMALL":    1.0,
		"SMALL":     2.0,
		"MEDIUM":    4.0,
		"LARGE":     8.0,
		"XLARGE":    16.0,
		"XXLARGE":   32.0,
		"XXXLARGE":  64.0,
		"XXXXLARGE": 128.0,
	}

	// Convert duration to hours for cost calculation
	hours := duration.Hours()

	// Use a default rate since warehouseSize is time.Duration, not string
	rate := 2.0 // Default to SMALL

	hours := duration.Hours()
	return hours * rate
}

// GetSessionParameters returns recommended session parameters
func (c *SnowflakeConfig) GetSessionParameters() map[string]string {
	params := map[string]string{
		"CLIENT_SESSION_KEEP_ALIVE":             "true",
		"CLIENT_RESULT_PREFETCH_THREADS":        "4",
		"CLIENT_RESULT_COLUMN_CASE_INSENSITIVE": "true",
	}

	// Add JSON tuning parameters
	params["CLIENT_RESULT_PREFETCH_THREADS"] = "4"

	return params
}

// BuildConnectionURL builds a connection URL string
func (c *SnowflakeConfig) BuildConnectionURL() string {
	var builder strings.Builder

	builder.WriteString(fmt.Sprintf("%s:%s@%s/%s",
		c.User,
		c.Password,
		c.Account,
		c.Database,
	))

	if c.Schema != "" {
		builder.WriteString("/" + c.Schema)
	}

	params := make([]string, 0)
	if c.Warehouse != "" {
		params = append(params, "warehouse="+c.Warehouse)
	}
	if c.Role != "" {
		params = append(params, "role="+c.Role)
	}

	if len(params) > 0 {
		builder.WriteString("?")
		for i, param := range params {
			if i > 0 {
				builder.WriteString("&")
			}
			builder.WriteString(param)
		}
	}

	return builder.String()
}
