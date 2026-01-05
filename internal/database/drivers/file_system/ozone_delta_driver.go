package file_system

import (
	"context"
	"database/sql"
	"fmt"
	"nexus-gateway/internal/database/drivers"

	"nexus-gateway/internal/database"
	"nexus-gateway/internal/model"
)

// OzoneDeltaDriver implements Driver interface for Ozone Delta Lake tables
type OzoneDeltaDriver struct {
	config *OzoneConfig
	client *OzoneClient
}

// NewOzoneDeltaDriver creates a new Ozone Delta Lake driver
func NewOzoneDeltaDriver(ctx context.Context, config *OzoneConfig) (*OzoneDeltaDriver, error) {
	client, err := NewOzoneClient(ctx, config)
	if err != nil {
		return nil, err
	}

	return &OzoneDeltaDriver{
		config: config,
		client: client,
	}, nil
}

// Open opens a connection (not applicable for Ozone Delta Lake)
func (d *OzoneDeltaDriver) Open(dsn string) (*sql.DB, error) {
	return nil, fmt.Errorf("Ozone Delta Lake driver does not support standard database connections")
}

// ValidateDSN validates the connection string
func (d *OzoneDeltaDriver) ValidateDSN(dsn string) error {
	if dsn == "" {
		return fmt.Errorf("DSN cannot be empty")
	}
	return nil
}

// GetDefaultPort returns the default Ozone port
func (d *OzoneDeltaDriver) GetDefaultPort() int {
	return 9880
}

// BuildDSN builds a connection string from configuration
func (d *OzoneDeltaDriver) BuildDSN(config *model.DataSourceConfig) string {
	return fmt.Sprintf("ozone://%s:%d/%s/%s",
		config.Host, config.Port, d.config.Volume, d.config.Bucket)
}

// GetDatabaseTypeName returns the database type name
func (d *OzoneDeltaDriver) GetDatabaseTypeName() string {
	return "ozone-delta"
}

// TestConnection tests if the connection is working
func (d *OzoneDeltaDriver) TestConnection(db *sql.DB) error {
	return fmt.Errorf("use TestConnectionContext instead")
}

// TestConnectionContext tests the Ozone connection
func (d *OzoneDeltaDriver) TestConnectionContext(ctx context.Context) error {
	_, err := d.client.GetCapacity(ctx)
	if err != nil {
		return fmt.Errorf("failed to test Ozone connection: %w", err)
	}
	return nil
}

// GetDriverName returns the driver name
func (d *OzoneDeltaDriver) GetDriverName() string {
	return "ozone-delta"
}

// GetCategory returns the driver category
func (d *OzoneDeltaDriver) GetCategory() drivers.DriverCategory {
	return drivers.CategoryFileSystem
}

// GetCapabilities returns driver capabilities
func (d *OzoneDeltaDriver) GetCapabilities() drivers.DriverCapabilities {
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
func (d *OzoneDeltaDriver) ConfigureAuth(authConfig interface{}) error {
	return nil
}

// GetTableMetadata retrieves Delta Lake table metadata
func (d *OzoneDeltaDriver) GetTableMetadata(ctx context.Context, tablePath string) (*DeltaMetadata, error) {
	logPath := tablePath + "/_delta_log/"
	data, err := d.client.ReadFile(ctx, logPath+"00000000000000000000.json")
	if err != nil {
		return nil, fmt.Errorf("failed to read delta log: %w", err)
	}

	// Parse Delta Lake transaction log (placeholder)
	return &DeltaMetadata{
		TableLocation: "ozone://" + d.config.Volume + "/" + d.config.Bucket + "/" + tablePath,
		Format:        "delta",
		Version:       0,
	}, nil
}

func (d *OzoneDeltaDriver) QueryTable(ctx context.Context, tablePath string, version int64) (*DeltaQueryResult, error) {
	// Query Delta Lake table (placeholder)
	return &DeltaQueryResult{
		Rows:    []map[string]interface{}{},
		NumRows: 0,
		Version: version,
	}, nil
}

// DeltaMetadata and DeltaQueryResult moved to common_types.go

// RegisterOzoneDeltaDriver registers the Ozone Delta Lake driver globally
func RegisterOzoneDeltaDriver(ctx context.Context, config *OzoneConfig) error {
	driver, err := NewOzoneDeltaDriver(ctx, config)
	if err != nil {
		return err
	}

	database.GetDriverRegistry().RegisterDriver(model.DatabaseTypeOzoneDelta, driver)
	return nil
}
