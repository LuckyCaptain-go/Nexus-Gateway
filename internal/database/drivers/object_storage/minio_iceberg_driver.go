package object_storage

import (
	"context"
	"database/sql"
	"fmt"

	"nexus-gateway/internal/database"
	"nexus-gateway/internal/model"
)

// MinIOIcebergDriver implements Driver interface for MinIO Iceberg tables
type MinIOIcebergDriver struct {
	config *MinIOConfig
	client *MinIOClient
}

// NewMinIOIcebergDriver creates a new MinIO Iceberg driver
func NewMinIOIcebergDriver(ctx context.Context, config *MinIOConfig) (*MinIOIcebergDriver, error) {
	client, err := NewMinIOClient(ctx, config)
	if err != nil {
		return nil, err
	}

	return &MinIOIcebergDriver{
		config: config,
		client: client,
	}, nil
}

// Open opens a connection (not applicable for MinIO Iceberg)
func (d *MinIOIcebergDriver) Open(dsn string) (*sql.DB, error) {
	return nil, fmt.Errorf("MinIO Iceberg driver does not support standard database connections")
}

// ValidateDSN validates the connection string
func (d *MinIOIcebergDriver) ValidateDSN(dsn string) error {
	if dsn == "" {
		return fmt.Errorf("DSN cannot be empty")
	}
	return nil
}

// GetDefaultPort returns the default MinIO port
func (d *MinIOIcebergDriver) GetDefaultPort() int {
	return 9000
}

// BuildDSN builds a connection string from configuration
func (d *MinIOIcebergDriver) BuildDSN(config *model.DataSourceConfig) string {
	return fmt.Sprintf("s3://%s", config.Bucket)
}

// GetDatabaseTypeName returns the database type name
func (d *MinIOIcebergDriver) GetDatabaseTypeName() string {
	return "minio-iceberg"
}

// TestConnection tests if the connection is working
func (d *MinIOIcebergDriver) TestConnection(db *sql.DB) error {
	return fmt.Errorf("use TestConnectionContext instead")
}

// TestConnectionContext tests the MinIO connection
func (d *MinIOIcebergDriver) TestConnectionContext(ctx context.Context) error {
	_, err := d.client.ListBuckets(ctx)
	if err != nil {
		return fmt.Errorf("failed to test MinIO connection: %w", err)
	}
	return nil
}

// GetDriverName returns the driver name
func (d *MinIOIcebergDriver) GetDriverName() string {
	return "minio-iceberg"
}

// GetCategory returns the driver category
func (d *MinIOIcebergDriver) GetCategory() database.DriverCategory {
	return database.CategoryObjectStorage
}

// GetCapabilities returns driver capabilities
func (d *MinIOIcebergDriver) GetCapabilities() database.DriverCapabilities {
	return database.DriverCapabilities{
		SupportsSQL:             false,
		SupportsTransaction:     true,
		SupportsSchemaDiscovery: true,
		SupportsTimeTravel:      true,
		RequiresTokenRotation:   false,
		SupportsStreaming:       true,
	}
}

// ConfigureAuth configures authentication
func (d *MinIOIcebergDriver) ConfigureAuth(authConfig interface{}) error {
	return nil
}

// RegisterMinIOIcebergDriver registers the MinIO Iceberg driver globally
func RegisterMinIOIcebergDriver(ctx context.Context, config *MinIOConfig) error {
	driver, err := NewMinIOIcebergDriver(ctx, config)
	if err != nil {
		return err
	}

	database.GetDriverRegistry().RegisterDriver(model.DatabaseTypeMinIOIceberg, driver)
	return nil
}
