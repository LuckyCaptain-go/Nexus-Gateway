package domestic

import (
	"context"
	"database/sql"
	"fmt"
	"nexus-gateway/internal/database/drivers"
	"strings"
	"time"

	"nexus-gateway/internal/model"

	_ "github.com/go-sql-driver/mysql"
)

// OceanBaseDriver implements Driver interface for OceanBase
type OceanBaseDriver struct {
	config     *OceanBaseConfig
	compatMode string // MYSQL or ORACLE
}

// OceanBaseConfig holds OceanBase configuration
type OceanBaseConfig struct {
	Host       string
	Port       int
	Database   string
	Username   string
	Password   string
	CompatMode string // MYSQL or ORACLE
}

// NewOceanBaseDriver creates a new OceanBase driver
func NewOceanBaseDriver(config *OceanBaseConfig) (*OceanBaseDriver, error) {
	if config.Host == "" {
		return nil, fmt.Errorf("host is required")
	}

	return &OceanBaseDriver{
		config:     config,
		compatMode: config.CompatMode,
	}, nil
}

// Open opens a connection to OceanBase
func (d *OceanBaseDriver) Open(dsn string) (*sql.DB, error) {
	driver := "mysql"
	if d.compatMode == "ORACLE" {
		// OceanBase Oracle mode would use different driver
		driver = "mysql" // For now, use MySQL driver
	}

	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open OceanBase connection: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping OceanBase: %w", err)
	}

	return db, nil
}

// ValidateDSN validates the connection string
func (d *OceanBaseDriver) ValidateDSN(dsn string) error {
	if dsn == "" {
		return fmt.Errorf("DSN cannot be empty")
	}
	return nil
}

// GetDefaultPort returns the default OceanBase port
func (d *OceanBaseDriver) GetDefaultPort() int {
	if d.compatMode == "ORACLE" {
		return 2883 // Oracle mode port
	}
	return 2881 // MySQL mode port
}

// BuildDSN builds a connection string from configuration
func (d *OceanBaseDriver) BuildDSN(config *model.DataSourceConfig) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		config.Username, config.Password, config.Host, config.Port, config.Database)
}

// GetDatabaseTypeName returns the database type name
func (d *OceanBaseDriver) GetDatabaseTypeName() string {
	if d.compatMode == "ORACLE" {
		return "oceanbase-oracle"
	}
	return "oceanbase-mysql"
}

// TestConnection tests if the connection is working
func (d *OceanBaseDriver) TestConnection(db *sql.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return db.PingContext(ctx)
}

// GetDriverName returns the driver name
func (d *OceanBaseDriver) GetDriverName() string {
	return "oceanbase"
}

// GetCategory returns the driver category
func (d *OceanBaseDriver) GetCategory() drivers.DriverCategory {
	return drivers.CategoryRelational
}

// GetCapabilities returns driver capabilities
func (d *OceanBaseDriver) GetCapabilities() drivers.DriverCapabilities {
	return drivers.DriverCapabilities{
		SupportsSQL:             true,
		SupportsTransaction:     true,
		SupportsSchemaDiscovery: true,
		SupportsTimeTravel:      false,
		RequiresTokenRotation:   false,
		SupportsStreaming:       false,
	}
}

// ConfigureAuth configures authentication
func (d *OceanBaseDriver) ConfigureAuth(authConfig interface{}) error {
	return nil
}

// GetCompatMode returns the compatibility mode
func (d *OceanBaseDriver) GetCompatMode() string {
	return d.compatMode
}

// DetectCompatMode detects compatibility mode from server version
func (d *OceanBaseDriver) DetectCompatMode(ctx context.Context, db *sql.DB) (string, error) {
	var version string
	err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version)
	if err != nil {
		return "", fmt.Errorf("failed to get version: %w", err)
	}

	// Detect mode from version string
	if contains(version, "OceanBase_CE") || contains(version, "OceanBase_ENT") {
		// Check if MySQL mode
		return "MYSQL", nil
	}

	// Default to MySQL mode
	return "MYSQL", nil
}


func contains(s, substr string) bool {
	return len(s) >= len(substr) && s[:len(substr)] == substr
}

// ApplyBatchPagination applies pagination to a SQL query for batch processing
func (d *OceanBaseDriver) ApplyBatchPagination(sql string, batchSize, offset int64) (string, error) {
	sql = strings.TrimSpace(sql)

	// Check if query already has LIMIT or OFFSET
	sqlUpper := strings.ToUpper(sql)
	if strings.Contains(sqlUpper, " LIMIT ") || strings.Contains(sqlUpper, " OFFSET ") {
		// For complex queries with existing pagination, add a subquery wrapper
		return fmt.Sprintf("SELECT * FROM (%%s) AS batch_query LIMIT %%d OFFSET %%d", sql, batchSize, offset), nil
	}
	// Use standard LIMIT/OFFSET for MySQL-compatible databases
	if offset > 0 {
		return fmt.Sprintf("%%s LIMIT %%d OFFSET %%d", sql, batchSize, offset), nil
	}
	return fmt.Sprintf("%%s LIMIT %%d", sql, batchSize), nil
}
