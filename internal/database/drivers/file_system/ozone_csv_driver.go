package file_system

import (
	"context"
	"database/sql"
	"fmt"

	"nexus-gateway/internal/database"
	"nexus-gateway/internal/model"
)

// OzoneCSVDriver implements Driver interface for Ozone CSV files
type OzoneCSVDriver struct {
	config *OzoneConfig
	client *OzoneClient
}

// NewOzoneCSVDriver creates a new Ozone CSV driver
func NewOzoneCSVDriver(ctx context.Context, config *OzoneConfig) (*OzoneCSVDriver, error) {
	client, err := NewOzoneClient(ctx, config)
	if err != nil {
		return nil, err
	}

	return &OzoneCSVDriver{
		config: config,
		client: client,
	}, nil
}

// Open opens a connection (not applicable for Ozone CSV)
func (d *OzoneCSVDriver) Open(dsn string) (*sql.DB, error) {
	return nil, fmt.Errorf("Ozone CSV driver does not support standard database connections")
}

// ValidateDSN validates the connection string
func (d *OzoneCSVDriver) ValidateDSN(dsn string) error {
	if dsn == "" {
		return fmt.Errorf("DSN cannot be empty")
	}
	return nil
}

// GetDefaultPort returns the default Ozone port
func (d *OzoneCSVDriver) GetDefaultPort() int {
	return 9880
}

// BuildDSN builds a connection string from configuration
func (d *OzoneCSVDriver) BuildDSN(config *model.DataSourceConfig) string {
	return fmt.Sprintf("ozone://%s:%d/%s/%s",
		config.Host, config.Port, d.config.Volume, d.config.Bucket)
}

// GetDatabaseTypeName returns the database type name
func (d *OzoneCSVDriver) GetDatabaseTypeName() string {
	return "ozone-csv"
}

// TestConnection tests if the connection is working
func (d *OzoneCSVDriver) TestConnection(db *sql.DB) error {
	return fmt.Errorf("use TestConnectionContext instead")
}

// TestConnectionContext tests the Ozone connection
func (d *OzoneCSVDriver) TestConnectionContext(ctx context.Context) error {
	_, err := d.client.GetCapacity(ctx)
	if err != nil {
		return fmt.Errorf("failed to test Ozone connection: %w", err)
	}
	return nil
}

// GetDriverName returns the driver name
func (d *OzoneCSVDriver) GetDriverName() string {
	return "ozone-csv"
}

// GetCategory returns the driver category
func (d *OzoneCSVDriver) GetCategory() database.DriverCategory {
	return database.CategoryFileSystem
}

// GetCapabilities returns driver capabilities
func (d *OzoneCSVDriver) GetCapabilities() database.DriverCapabilities {
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
func (d *OzoneCSVDriver) ConfigureAuth(authConfig interface{}) error {
	return nil
}

// ListCSVFiles lists CSV files in a bucket
func (d *OzoneCSVDriver) ListCSVFiles(ctx context.Context, prefix string) ([]OzoneFileInfo, error) {
	return d.client.ListFiles(ctx, prefix)
}

// QueryCSVFile queries a CSV file from Ozone
func (d *OzoneCSVDriver) QueryCSVFile(ctx context.Context, key string, delimiter rune, hasHeader bool) (*OzoneCSVResult, error) {
	data, err := d.client.ReadFile(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV file: %w", err)
	}

	// Parse CSV file (placeholder)
	return &OzoneCSVResult{
		Rows:      []map[string]interface{}{},
		NumRows:   0,
		BytesRead: int64(len(data)),
		Key:       key,
	}, nil
}

// OzoneCSVResult represents query result
type OzoneCSVResult struct {
	Rows      []map[string]interface{}
	NumRows   int64
	BytesRead int64
	Key       string
}

// RegisterOzoneCSVDriver registers the Ozone CSV driver globally
func RegisterOzoneCSVDriver(ctx context.Context, config *OzoneConfig) error {
	driver, err := NewOzoneCSVDriver(ctx, config)
	if err != nil {
		return err
	}

	database.GetDriverRegistry().RegisterDriver(model.DatabaseTypeOzoneCSV, driver)
	return nil
}
