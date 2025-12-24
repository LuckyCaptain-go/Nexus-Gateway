package file_system

import (
	"context"
	"database/sql"
	"fmt"

	"nexus-gateway/internal/database"
	"nexus-gateway/internal/model"
)

// OzoneAvroDriver implements Driver interface for Ozone Avro files
type OzoneAvroDriver struct {
	config *OzoneConfig
	client *OzoneClient
}

// NewOzoneAvroDriver creates a new Ozone Avro driver
func NewOzoneAvroDriver(ctx context.Context, config *OzoneConfig) (*OzoneAvroDriver, error) {
	client, err := NewOzoneClient(ctx, config)
	if err != nil {
		return nil, err
	}

	return &OzoneAvroDriver{
		config: config,
		client: client,
	}, nil
}

// Open opens a connection (not applicable for Ozone Avro)
func (d *OzoneAvroDriver) Open(dsn string) (*sql.DB, error) {
	return nil, fmt.Errorf("Ozone Avro driver does not support standard database connections")
}

// ValidateDSN validates the connection string
func (d *OzoneAvroDriver) ValidateDSN(dsn string) error {
	if dsn == "" {
		return fmt.Errorf("DSN cannot be empty")
	}
	return nil
}

// GetDefaultPort returns the default Ozone port
func (d *OzoneAvroDriver) GetDefaultPort() int {
	return 9880
}

// BuildDSN builds a connection string from configuration
func (d *OzoneAvroDriver) BuildDSN(config *model.DataSourceConfig) string {
	return fmt.Sprintf("ozone://%s:%d/%s/%s",
		config.Host, config.Port, d.config.Volume, d.config.Bucket)
}

// GetDatabaseTypeName returns the database type name
func (d *OzoneAvroDriver) GetDatabaseTypeName() string {
	return "ozone-avro"
}

// TestConnection tests if the connection is working
func (d *OzoneAvroDriver) TestConnection(db *sql.DB) error {
	return fmt.Errorf("use TestConnectionContext instead")
}

// TestConnectionContext tests the Ozone connection
func (d *OzoneAvroDriver) TestConnectionContext(ctx context.Context) error {
	_, err := d.client.GetCapacity(ctx)
	if err != nil {
		return fmt.Errorf("failed to test Ozone connection: %w", err)
	}
	return nil
}

// GetDriverName returns the driver name
func (d *OzoneAvroDriver) GetDriverName() string {
	return "ozone-avro"
}

// GetCategory returns the driver category
func (d *OzoneAvroDriver) GetCategory() database.DriverCategory {
	return database.CategoryFileSystem
}

// GetCapabilities returns driver capabilities
func (d *OzoneAvroDriver) GetCapabilities() database.DriverCapabilities {
	return database.DriverCapabilities{
		SupportsSQL:             false,
		SupportsTransaction:     false,
		SupportsSchemaDiscovery: true,
		SupportsTimeTravel:      false,
		RequiresTokenRotation:   false,
		SupportsStreaming:       true,
	}
}

// ConfigureAuth configures authentication
func (d *OzoneAvroDriver) ConfigureAuth(authConfig interface{}) error {
	return nil
}

// ListAvroFiles lists Avro files in a bucket
func (d *OzoneAvroDriver) ListAvroFiles(ctx context.Context, prefix string) ([]OzoneFileInfo, error) {
	return d.client.ListFiles(ctx, prefix)
}

// QueryAvroFile queries an Avro file from Ozone
func (d *OzoneAvroDriver) QueryAvroFile(ctx context.Context, key string) (*OzoneAvroResult, error) {
	data, err := d.client.ReadFile(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to read Avro file: %w", err)
	}

	// Parse Avro file (placeholder)
	return &OzoneAvroResult{
		Rows:     []map[string]interface{}{},
		NumRows:  0,
		BytesRead: int64(len(data)),
		Key:      key,
	}, nil
}

// OzoneAvroResult represents query result
type OzoneAvroResult struct {
	Rows      []map[string]interface{}
	NumRows   int64
	BytesRead int64
	Key       string
}

// RegisterOzoneAvroDriver registers the Ozone Avro driver globally
func RegisterOzoneAvroDriver(ctx context.Context, config *OzoneConfig) error {
	driver, err := NewOzoneAvroDriver(ctx, config)
	if err != nil {
		return err
	}

	database.GetDriverRegistry().RegisterDriver(model.DatabaseTypeOzoneAvro, driver)
	return nil
}
