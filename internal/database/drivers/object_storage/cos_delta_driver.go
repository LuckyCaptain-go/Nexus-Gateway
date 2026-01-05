package object_storage

import (
	"context"
	"database/sql"
	"fmt"
	"nexus-gateway/internal/database/drivers"

	"nexus-gateway/internal/database"
	"nexus-gateway/internal/model"
)

// COSDeltaDriver implements Driver interface for Tencent COS Delta Lake tables
type COSDeltaDriver struct {
	config *COSConfig
	client *COSClient
}

// NewCOSDeltaDriver creates a new COS Delta Lake driver
func NewCOSDeltaDriver(ctx context.Context, config *COSConfig) (*COSDeltaDriver, error) {
	client, err := NewCOSClient(ctx, config)
	if err != nil {
		return nil, err
	}

	return &COSDeltaDriver{
		config: config,
		client: client,
	}, nil
}

// Open opens a connection (not applicable for COS Delta Lake)
func (d *COSDeltaDriver) Open(dsn string) (*sql.DB, error) {
	return nil, fmt.Errorf("COS Delta Lake driver does not support standard database connections")
}

// ValidateDSN validates the connection string
func (d *COSDeltaDriver) ValidateDSN(dsn string) error {
	if dsn == "" {
		return fmt.Errorf("DSN cannot be empty")
	}
	return nil
}

// GetDefaultPort returns the default COS port
func (d *COSDeltaDriver) GetDefaultPort() int {
	return 443 // HTTPS
}

// BuildDSN builds a connection string from configuration
func (d *COSDeltaDriver) BuildDSN(config *model.DataSourceConfig) string {
	return fmt.Sprintf("cos://%s", config.BucketName)
}

// GetDatabaseTypeName returns the database type name
func (d *COSDeltaDriver) GetDatabaseTypeName() string {
	return "cos-delta"
}

// TestConnection tests if the connection is working
func (d *COSDeltaDriver) TestConnection(db *sql.DB) error {
	return fmt.Errorf("use TestConnectionContext instead")
}

// TestConnectionContext tests the COS connection
func (d *COSDeltaDriver) TestConnectionContext(ctx context.Context) error {
	_, err := d.client.ListObjects(ctx, "", 1)
	if err != nil {
		return fmt.Errorf("failed to test COS connection: %w", err)
	}
	return nil
}

// GetDriverName returns the driver name
func (d *COSDeltaDriver) GetDriverName() string {
	return "cos-delta"
}

// GetCategory returns the driver category
func (d *COSDeltaDriver) GetCategory() drivers.DriverCategory {
	return drivers.CategoryObjectStorage
}

// GetCapabilities returns driver capabilities
func (d *COSDeltaDriver) GetCapabilities() drivers.DriverCapabilities {
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
func (d *COSDeltaDriver) ConfigureAuth(authConfig interface{}) error {
	return nil
}

// RegisterCOSDeltaDriver registers the COS Delta Lake driver globally
func RegisterCOSDeltaDriver(ctx context.Context, config *COSConfig) error {
	driver, err := NewCOSDeltaDriver(ctx, config)
	if err != nil {
		return err
	}

	database.GetDriverRegistry().RegisterDriver(model.DatabaseTypeCOSDelta, driver)
	return nil
}
