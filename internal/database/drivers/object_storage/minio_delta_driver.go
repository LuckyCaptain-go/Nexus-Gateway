package object_storage

import (
	"context"
	"database/sql"
	"fmt"
	"nexus-gateway/internal/database/drivers"

	"nexus-gateway/internal/model"
)

// MinIODeltaDriver implements Driver interface for MinIO Delta Lake tables
type MinIODeltaDriver struct {
	config *MinIOConfig
	client *MinIOClient
}

// NewMinIODeltaDriver creates a new MinIO Delta Lake driver
func NewMinIODeltaDriver(ctx context.Context, config *MinIOConfig) (*MinIODeltaDriver, error) {
	client, err := NewMinIOClient(ctx, config)
	if err != nil {
		return nil, err
	}

	return &MinIODeltaDriver{
		config: config,
		client: client,
	}, nil
}

// Open opens a connection (not applicable for MinIO Delta Lake)
func (d *MinIODeltaDriver) Open(dsn string) (*sql.DB, error) {
	return nil, fmt.Errorf("MinIO Delta Lake driver does not support standard database connections")
}

// ValidateDSN validates the connection string
func (d *MinIODeltaDriver) ValidateDSN(dsn string) error {
	if dsn == "" {
		return fmt.Errorf("DSN cannot be empty")
	}
	return nil
}

// GetDefaultPort returns the default MinIO port
func (d *MinIODeltaDriver) GetDefaultPort() int {
	return 9000
}

// BuildDSN builds a connection string from configuration
func (d *MinIODeltaDriver) BuildDSN(config *model.DataSourceConfig) string {
	return fmt.Sprintf("s3://%s", config.Database)
}

// GetDatabaseTypeName returns the database type name
func (d *MinIODeltaDriver) GetDatabaseTypeName() string {
	return "minio-delta"
}

// TestConnection tests if the connection is working
func (d *MinIODeltaDriver) TestConnection(db *sql.DB) error {
	return fmt.Errorf("use TestConnectionContext instead")
}

// TestConnectionContext tests the MinIO connection
func (d *MinIODeltaDriver) TestConnectionContext(ctx context.Context) error {
	_, err := d.client.ListBuckets(ctx)
	if err != nil {
		return fmt.Errorf("failed to test MinIO connection: %w", err)
	}
	return nil
}

// GetDriverName returns the driver name
func (d *MinIODeltaDriver) GetDriverName() string {
	return "minio-delta"
}

// GetCategory returns the driver category
func (d *MinIODeltaDriver) GetCategory() drivers.DriverCategory {
	return drivers.CategoryObjectStorage
}

// GetCapabilities returns driver capabilities
func (d *MinIODeltaDriver) GetCapabilities() drivers.DriverCapabilities {
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
func (d *MinIODeltaDriver) ConfigureAuth(authConfig interface{}) error {
	return nil
}

