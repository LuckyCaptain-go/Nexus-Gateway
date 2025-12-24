package file_system

import (
	"context"
	"database/sql"
	"fmt"

	"nexus-gateway/internal/database"
	"nexus-gateway/internal/model"
)

// HDFSAvroDriver implements Driver interface for HDFS Avro files
type HDFSAvroDriver struct {
	config *HDFSConfig
	client *HDFSClient
}

// NewHDFSAvroDriver creates a new HDFS Avro driver
func NewHDFSAvroDriver(ctx context.Context, config *HDFSConfig) (*HDFSAvroDriver, error) {
	client, err := NewHDFSClient(ctx, config)
	if err != nil {
		return nil, err
	}

	return &HDFSAvroDriver{
		config: config,
		client: client,
	}, nil
}

// Open opens a connection (not applicable for HDFS Avro)
func (d *HDFSAvroDriver) Open(dsn string) (*sql.DB, error) {
	return nil, fmt.Errorf("HDFS Avro driver does not support standard database connections")
}

// ValidateDSN validates the connection string
func (d *HDFSAvroDriver) ValidateDSN(dsn string) error {
	if dsn == "" {
		return fmt.Errorf("DSN cannot be empty")
	}
	return nil
}

// GetDefaultPort returns the default HDFS port
func (d *HDFSAvroDriver) GetDefaultPort() int {
	return 8020
}

// BuildDSN builds a connection string from configuration
func (d *HDFSAvroDriver) BuildDSN(config *model.DataSourceConfig) string {
	return fmt.Sprintf("hdfs://%s:8020", config.Host)
}

// GetDatabaseTypeName returns the database type name
func (d *HDFSAvroDriver) GetDatabaseTypeName() string {
	return "hdfs-avro"
}

// TestConnection tests if the connection is working
func (d *HDFSAvroDriver) TestConnection(db *sql.DB) error {
	return fmt.Errorf("use TestConnectionContext instead")
}

// TestConnectionContext tests the HDFS connection
func (d *HDFSAvroDriver) TestConnectionContext(ctx context.Context) error {
	_, err := d.client.GetCapacity(ctx)
	if err != nil {
		return fmt.Errorf("failed to test HDFS connection: %w", err)
	}
	return nil
}

// GetDriverName returns the driver name
func (d *HDFSAvroDriver) GetDriverName() string {
	return "hdfs-avro"
}

// GetCategory returns the driver category
func (d *HDFSAvroDriver) GetCategory() database.DriverCategory {
	return database.CategoryFileSystem
}

// GetCapabilities returns driver capabilities
func (d *HDFSAvroDriver) GetCapabilities() database.DriverCapabilities {
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
func (d *HDFSAvroDriver) ConfigureAuth(authConfig interface{}) error {
	return nil
}

// ListAvroFiles lists Avro files in a directory
func (d *HDFSAvroDriver) ListAvroFiles(ctx context.Context, path string) ([]HDFSFileInfo, error) {
	files, err := d.client.ListFiles(ctx, path)
	if err != nil {
		return nil, err
	}

	var avroFiles []HDFSFileInfo
	for _, file := range files {
		if !file.IsDir && len(file.Name) > 5 && file.Name[len(file.Name)-5:] == ".avro" {
			avroFiles = append(avroFiles, file)
		}
	}

	return avroFiles, nil
}

// QueryAvroFile queries an Avro file from HDFS
func (d *HDFSAvroDriver) QueryAvroFile(ctx context.Context, path string) (*HDFSAvroResult, error) {
	data, err := d.client.ReadFile(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("failed to read Avro file: %w", err)
	}

	// Parse Avro file (placeholder)
	return &HDFSAvroResult{
		Rows:     []map[string]interface{}{},
		NumRows:  0,
		BytesRead: int64(len(data)),
		FilePath: path,
	}, nil
}

// HDFSAvroResult represents query result
type HDFSAvroResult struct {
	Rows      []map[string]interface{}
	NumRows   int64
	BytesRead int64
	FilePath  string
}

// RegisterHDFSAvroDriver registers the HDFS Avro driver globally
func RegisterHDFSAvroDriver(ctx context.Context, config *HDFSConfig) error {
	driver, err := NewHDFSAvroDriver(ctx, config)
	if err != nil {
		return err
	}

	database.GetDriverRegistry().RegisterDriver(model.DatabaseTypeHDFSAvro, driver)
	return nil
}
