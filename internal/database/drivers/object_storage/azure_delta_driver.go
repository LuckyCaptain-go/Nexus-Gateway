package object_storage

import (
	"context"
	"database/sql"
	"fmt"
	"nexus-gateway/internal/database/drivers"

	"nexus-gateway/internal/database"
	"nexus-gateway/internal/model"
)

// AzureDeltaDriver implements Driver interface for Azure Blob Delta Lake tables
type AzureDeltaDriver struct {
	config *AzureBlobConfig
	client *AzureBlobClient
}

// NewAzureDeltaDriver creates a new Azure Delta Lake driver
func NewAzureDeltaDriver(ctx context.Context, config *AzureBlobConfig) (*AzureDeltaDriver, error) {
	client, err := NewAzureBlobClient(ctx, config)
	if err != nil {
		return nil, err
	}

	return &AzureDeltaDriver{
		config: config,
		client: client,
	}, nil
}

// Open opens a connection (not applicable for Azure Delta Lake)
func (d *AzureDeltaDriver) Open(dsn string) (*sql.DB, error) {
	return nil, fmt.Errorf("Azure Delta Lake driver does not support standard database connections")
}

// ValidateDSN validates the connection string
func (d *AzureDeltaDriver) ValidateDSN(dsn string) error {
	if dsn == "" {
		return fmt.Errorf("DSN cannot be empty")
	}
	return nil
}

// GetDefaultPort returns the default Azure Blob port
func (d *AzureDeltaDriver) GetDefaultPort() int {
	return 443 // HTTPS
}

// BuildDSN builds a connection string from configuration
func (d *AzureDeltaDriver) BuildDSN(config *model.DataSourceConfig) string {
	return fmt.Sprintf("azure://%s/%s", config.Database, config.Host)
}

// GetDatabaseTypeName returns the database type name
func (d *AzureDeltaDriver) GetDatabaseTypeName() string {
	return "azure-delta"
}

// TestConnection tests if the connection is working
func (d *AzureDeltaDriver) TestConnection(db *sql.DB) error {
	return fmt.Errorf("use TestConnectionContext instead")
}

// TestConnectionContext tests the Azure Blob connection
func (d *AzureDeltaDriver) TestConnectionContext(ctx context.Context) error {
	_, err := d.client.ListBlobs(ctx, "", 1)
	if err != nil {
		return fmt.Errorf("failed to test Azure connection: %w", err)
	}
	return nil
}

// GetDriverName returns the driver name
func (d *AzureDeltaDriver) GetDriverName() string {
	return "azure-delta"
}

// GetCategory returns the driver category
func (d *AzureDeltaDriver) GetCategory() drivers.DriverCategory {
	return drivers.CategoryObjectStorage
}

// GetCapabilities returns driver capabilities
func (d *AzureDeltaDriver) GetCapabilities() drivers.DriverCapabilities {
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
func (d *AzureDeltaDriver) ConfigureAuth(authConfig interface{}) error {
	return nil
}

// RegisterAzureDeltaDriver registers the Azure Delta Lake driver globally
func RegisterAzureDeltaDriver(ctx context.Context, config *AzureBlobConfig) error {
	driver, err := NewAzureDeltaDriver(ctx, config)
	if err != nil {
		return err
	}

	database.GetDriverRegistry().RegisterDriver(model.DatabaseTypeAzureDelta, driver)
	return nil
}
