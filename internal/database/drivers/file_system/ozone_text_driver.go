package file_system

import (
	"context"
	"database/sql"
	"fmt"
	"nexus-gateway/internal/database/drivers"

	"nexus-gateway/internal/model"
)

// OzoneTextDriver implements Driver interface for Ozone text files
type OzoneTextDriver struct {
	config *OzoneConfig
	client *OzoneClient
}

func (d *OzoneTextDriver) ApplyBatchPagination(sql string, batchSize, offset int64) (string, error) {
	//TODO implement me
	panic("implement me")
}

// NewOzoneTextDriver creates a new Ozone text driver
func NewOzoneTextDriver(ctx context.Context, config *OzoneConfig) (*OzoneTextDriver, error) {
	client, err := NewOzoneClient(ctx, config)
	if err != nil {
		return nil, err
	}

	return &OzoneTextDriver{
		config: config,
		client: client,
	}, nil
}

// Open opens a connection (not applicable for Ozone text)
func (d *OzoneTextDriver) Open(dsn string) (*sql.DB, error) {
	return nil, fmt.Errorf("Ozone text driver does not support standard database connections")
}

// ValidateDSN validates the connection string
func (d *OzoneTextDriver) ValidateDSN(dsn string) error {
	if dsn == "" {
		return fmt.Errorf("DSN cannot be empty")
	}
	return nil
}

// GetDefaultPort returns the default Ozone port
func (d *OzoneTextDriver) GetDefaultPort() int {
	return 9880
}

// BuildDSN builds a connection string from configuration
func (d *OzoneTextDriver) BuildDSN(config *model.DataSourceConfig) string {
	return fmt.Sprintf("ozone://%s:%d/%s/%s",
		config.Host, config.Port, d.config.Volume, d.config.Bucket)
}

// GetDatabaseTypeName returns the database type name
func (d *OzoneTextDriver) GetDatabaseTypeName() string {
	return "ozone-text"
}

// TestConnection tests if the connection is working
func (d *OzoneTextDriver) TestConnection(db *sql.DB) error {
	return fmt.Errorf("use TestConnectionContext instead")
}

// TestConnectionContext tests the Ozone connection
func (d *OzoneTextDriver) TestConnectionContext(ctx context.Context) error {
	_, err := d.client.GetCapacity(ctx)
	if err != nil {
		return fmt.Errorf("failed to test Ozone connection: %w", err)
	}
	return nil
}

// GetDriverName returns the driver name
func (d *OzoneTextDriver) GetDriverName() string {
	return "ozone-text"
}

// GetCategory returns the driver category
func (d *OzoneTextDriver) GetCategory() drivers.DriverCategory {
	return drivers.CategoryFileSystem
}

// GetCapabilities returns driver capabilities
func (d *OzoneTextDriver) GetCapabilities() drivers.DriverCapabilities {
	return drivers.DriverCapabilities{
		SupportsSQL:             false,
		SupportsTransaction:     false,
		SupportsSchemaDiscovery: true,
		SupportsTimeTravel:      false,
		RequiresTokenRotation:   false,
		SupportsStreaming:       true,
	}
}

// ConfigureAuth configures authentication
func (d *OzoneTextDriver) ConfigureAuth(authConfig interface{}) error {
	return nil
}

// ListTextFiles lists text files in a bucket
func (d *OzoneTextDriver) ListTextFiles(ctx context.Context, prefix string) ([]OzoneFileInfo, error) {
	return d.client.ListFiles(ctx, prefix)
}

// QueryTextFile queries a text file from Ozone
func (d *OzoneTextDriver) QueryTextFile(ctx context.Context, key string) (*OzoneTextResult, error) {
	data, err := d.client.ReadFile(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to read text file: %w", err)
	}

	// Parse text file (placeholder)
	return &OzoneTextResult{
		Rows:      []string{string(data)},
		LineCount: 1,
		BytesRead: int64(len(data)),
		Key:       key,
	}, nil
}

// OzoneTextResult represents query result
type OzoneTextResult struct {
	Rows      []string
	LineCount int
	BytesRead int64
	Key       string
}

// RegisterOzoneTextDriver registers the Ozone text driver globally
