package object_storage

import (
	"context"
	"database/sql"
	"fmt"
	"nexus-gateway/internal/database/drivers"

	"nexus-gateway/internal/model"
)

// OSSDeltaDriver implements Driver interface for Alibaba OSS Delta Lake tables
type OSSDeltaDriver struct {
	config *OSSConfig
	client *OSSClient
}

// NewOSSDeltaDriver creates a new OSS Delta Lake driver
func NewOSSDeltaDriver(ctx context.Context, config *OSSConfig) (*OSSDeltaDriver, error) {
	client, err := NewOSSClient(ctx, config)
	if err != nil {
		return nil, err
	}

	return &OSSDeltaDriver{
		config: config,
		client: client,
	}, nil
}

// Open opens a connection (not applicable for OSS Delta Lake)
func (d *OSSDeltaDriver) Open(dsn string) (*sql.DB, error) {
	return nil, fmt.Errorf("OSS Delta Lake driver does not support standard database connections")
}

// ValidateDSN validates the connection string
func (d *OSSDeltaDriver) ValidateDSN(dsn string) error {
	if dsn == "" {
		return fmt.Errorf("DSN cannot be empty")
	}
	return nil
}

// GetDefaultPort returns the default OSS port
func (d *OSSDeltaDriver) GetDefaultPort() int {
	return 443 // HTTPS
}

// BuildDSN builds a connection string from configuration
func (d *OSSDeltaDriver) BuildDSN(config *model.DataSourceConfig) string {
	return fmt.Sprintf("oss://%s", config.Database)
}

// GetDatabaseTypeName returns the database type name
func (d *OSSDeltaDriver) GetDatabaseTypeName() string {
	return "oss-delta"
}

// TestConnection tests if the connection is working
func (d *OSSDeltaDriver) TestConnection(db *sql.DB) error {
	return fmt.Errorf("use TestConnectionContext instead")
}

// TestConnectionContext tests the OSS connection
func (d *OSSDeltaDriver) TestConnectionContext(ctx context.Context) error {
	_, err := d.client.ListObjects(ctx, "", 1)
	if err != nil {
		return fmt.Errorf("failed to test OSS connection: %w", err)
	}
	return nil
}

// GetDriverName returns the driver name
func (d *OSSDeltaDriver) GetDriverName() string {
	return "oss-delta"
}

// GetCategory returns the driver category
func (d *OSSDeltaDriver) GetCategory() drivers.DriverCategory {
	return drivers.CategoryObjectStorage
}

// GetCapabilities returns driver capabilities
func (d *OSSDeltaDriver) GetCapabilities() drivers.DriverCapabilities {
	return drivers.DriverCapabilities{
		SupportsSQL:             false,
		SupportsTransaction:     true,
		SupportsSchemaDiscovery: true,
		SupportsTimeTravel:      true,
		RequiresTokenRotation:   false,
		SupportsStreaming:       true,
	}
}

// ConfigureAuth configures authentication
func (d *OSSDeltaDriver) ConfigureAuth(authConfig interface{}) error {
	return nil
}

// ApplyBatchPagination adds pagination to SQL query
func (d *OSSDeltaDriver) ApplyBatchPagination(sql string, batchSize, offset int64) (string, error) {
	// For OSS Delta files, pagination is typically not supported in the same way as traditional databases
	// We return the original SQL as-is since Delta files don't support LIMIT/OFFSET in the same way
	// The pagination is usually handled at the application level
	return sql, nil
}

