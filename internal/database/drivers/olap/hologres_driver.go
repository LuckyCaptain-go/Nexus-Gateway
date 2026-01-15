package olap

import (
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	"nexus-gateway/internal/database/drivers"
	"nexus-gateway/internal/model"

	_ "github.com/lib/pq" // PostgreSQL driver for Hologres (Hologres is PostgreSQL-compatible)
)

// HologresDriver implements the Driver interface for Alibaba Cloud Hologres
type HologresDriver struct {
	base *drivers.DriverBase
}

// ApplyBatchPagination applies pagination to SQL query for batch processing
func (hd *HologresDriver) ApplyBatchPagination(sql string, batchSize, offset int64) (string, error) {
	return fmt.Sprintf("%s LIMIT %d OFFSET %d", sql, batchSize, offset), nil
}

// NewHologresDriver creates a new Hologres driver instance
func NewHologresDriver() *HologresDriver {
	return &HologresDriver{
		base: drivers.NewDriverBase(model.DatabaseTypeHologres, drivers.CategoryOLAP),
	}
}

// Open opens a connection to Hologres database
func (hd *HologresDriver) Open(dsn string) (*sql.DB, error) {
	// Hologres is PostgreSQL-compatible, so we can use the PostgreSQL driver
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Hologres: %v", err)
	}

	return db, nil
}

// ValidateDSN validates the Hologres connection string
func (hd *HologresDriver) ValidateDSN(dsn string) error {
	// Parse DSN and validate its format
	parsedURL, err := url.Parse(dsn)
	if err != nil {
		return fmt.Errorf("invalid DSN format: %v", err)
	}

	// Check if it's using postgresql scheme (since Hologres is PostgreSQL-compatible)
	scheme := strings.ToLower(parsedURL.Scheme)
	if scheme != "postgresql" && scheme != "postgres" {
		return fmt.Errorf("invalid scheme in DSN: %s, expected 'postgresql' or 'postgres'", scheme)
	}

	if parsedURL.Host == "" {
		return fmt.Errorf("missing host in DSN")
	}

	if parsedURL.User == nil {
		return fmt.Errorf("missing username in DSN")
	}

	password, ok := parsedURL.User.Password()
	if !ok {
		return fmt.Errorf("missing password in DSN")
	}

	if password == "" {
		return fmt.Errorf("empty password in DSN")
	}

	return nil
}

// GetDefaultPort returns the default port for Hologres
func (hd *HologresDriver) GetDefaultPort() int {
	return 5432 // Default PostgreSQL/Hologres port
}

// BuildDSN builds a Hologres connection string from configuration
func (hd *HologresDriver) BuildDSN(config *model.DataSourceConfig) string {
	// Build Hologres connection string (uses PostgreSQL-compatible format)
	// Format: postgresql://user:password@host:port/database?sslmode=disable
	var sslMode string
	if config.SSL {
		sslMode = "require"
	} else {
		sslMode = "disable"
	}

	dsn := fmt.Sprintf("postgresql://%s:%s@%s:%d/%s?sslmode=%s",
		config.Username,
		url.QueryEscape(config.Password),
		config.Host,
		config.Port,
		config.Database,
		sslMode,
	)

	// Add additional properties
	if config.AdditionalProps != nil {
		params := url.Values{}
		for k, v := range config.AdditionalProps {
			params.Add(k, fmt.Sprintf("%v", v))
		}
		// Append additional params to the DSN
		if params.Encode() != "" {
			dsn += "&" + params.Encode()
		}
	}

	return dsn
}

// GetDatabaseTypeName returns the database type name
func (hd *HologresDriver) GetDatabaseTypeName() string {
	return hd.base.GetDatabaseTypeName()
}

// TestConnection tests if the Hologres connection is working
func (hd *HologresDriver) TestConnection(db *sql.DB) error {
	// Execute a simple query to test the connection
	query := "SELECT 1"
	row := db.QueryRow(query)
	var result int
	err := row.Scan(&result)
	if err != nil {
		return fmt.Errorf("failed to execute test query: %v", err)
	}

	if result != 1 {
		return fmt.Errorf("unexpected result from test query: %d", result)
	}

	return nil
}

// GetDriverName returns the underlying SQL driver name for Hologres
func (hd *HologresDriver) GetDriverName() string {
	return "postgres"
}

// GetCategory returns the driver category
func (hd *HologresDriver) GetCategory() drivers.DriverCategory {
	return hd.base.GetCategory()
}

// GetCapabilities returns driver capabilities
func (hd *HologresDriver) GetCapabilities() drivers.DriverCapabilities {
	return drivers.DriverCapabilities{
		SupportsSQL:             true,
		SupportsTransaction:     true,  // Hologres supports ACID transactions
		SupportsSchemaDiscovery: true,  // Hologres supports schema introspection
		SupportsTimeTravel:      false, // Hologres doesn't have built-in time travel like some other systems
		RequiresTokenRotation:   false, // Hologres uses traditional authentication
		SupportsStreaming:       true,  // Supports streaming of large result sets
	}
}

// ConfigureAuth configures authentication for Hologres
func (hd *HologresDriver) ConfigureAuth(authConfig interface{}) error {
	// Hologres supports various authentication methods like password authentication, etc.
	// Implementation would depend on specific authConfig provided
	// This is a placeholder for future extension

	return nil
}
