package file_system

import (
	"context"
	"database/sql"
	"fmt"

	"nexus-gateway/internal/database"
	"nexus-gateway/internal/model"
)

// HDFSORCDriver implements Driver interface for HDFS ORC files
type HDFSORCDriver struct {
	config *HDFSConfig
	client *HDFSClient
}

// NewHDFSORCDriver creates a new HDFS ORC driver
func NewHDFSORCDriver(ctx context.Context, config *HDFSConfig) (*HDFSORCDriver, error) {
	client, err := NewHDFSClient(ctx, config)
	if err != nil {
		return nil, err
	}

	return &HDFSORCDriver{
		config: config,
		client: client,
	}, nil
}

// Open opens a connection (not applicable for HDFS ORC)
func (d *HDFSORCDriver) Open(dsn string) (*sql.DB, error) {
	return nil, fmt.Errorf("HDFS ORC driver does not support standard database connections")
}

// ValidateDSN validates the connection string
func (d *HDFSORCDriver) ValidateDSN(dsn string) error {
	if dsn == "" {
		return fmt.Errorf("DSN cannot be empty")
	}
	return nil
}

// GetDefaultPort returns the default HDFS port
func (d *HDFSORCDriver) GetDefaultPort() int {
	return 8020
}

// BuildDSN builds a connection string from configuration
func (d *HDFSORCDriver) BuildDSN(config *model.DataSourceConfig) string {
	return fmt.Sprintf("hdfs://%s:8020", config.Host)
}

// GetDatabaseTypeName returns the database type name
func (d *HDFSORCDriver) GetDatabaseTypeName() string {
	return "hdfs-orc"
}

// TestConnection tests if the connection is working
func (d *HDFSORCDriver) TestConnection(db *sql.DB) error {
	return fmt.Errorf("use TestConnectionContext instead")
}

// TestConnectionContext tests the HDFS connection
func (d *HDFSORCDriver) TestConnectionContext(ctx context.Context) error {
	_, err := d.client.GetCapacity(ctx)
	if err != nil {
		return fmt.Errorf("failed to test HDFS connection: %w", err)
	}
	return nil
}

// GetDriverName returns the driver name
func (d *HDFSORCDriver) GetDriverName() string {
	return "hdfs-orc"
}

// GetCategory returns the driver category
func (d *HDFSORCDriver) GetCategory() database.DriverCategory {
	return database.CategoryFileSystem
}

// GetCapabilities returns driver capabilities
func (d *HDFSORCDriver) GetCapabilities() database.DriverCapabilities {
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
func (d *HDFSORCDriver) ConfigureAuth(authConfig interface{}) error {
	return nil
}

// ListORCFiles lists ORC files in a directory
func (d *HDFSORCDriver) ListORCFiles(ctx context.Context, path string) ([]HDFSFileInfo, error) {
	files, err := d.client.ListFiles(ctx, path)
	if err != nil {
		return nil, err
	}

	var orcFiles []HDFSFileInfo
	for _, file := range files {
		if !file.IsDir && len(file.Name) > 4 && file.Name[len(file.Name)-4:] == ".orc" {
			orcFiles = append(orcFiles, file)
		}
	}

	return orcFiles, nil
}

// QueryORCFile queries an ORC file from HDFS
func (d *HDFSORCDriver) QueryORCFile(ctx context.Context, path string) (*HDFSORCResult, error) {
	data, err := d.client.ReadFile(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("failed to read ORC file: %w", err)
	}

	// Parse ORC file (placeholder)
	return &HDFSORCResult{
		Rows:     []map[string]interface{}{},
		NumRows:  0,
		BytesRead: int64(len(data)),
		FilePath: path,
	}, nil
}

// HDFSORCResult represents query result
type HDFSORCResult struct {
	Rows      []map[string]interface{}
	NumRows   int64
	BytesRead int64
	FilePath  string
}

// RegisterHDFSORCDriver registers the HDFS ORC driver globally
func RegisterHDFSORCDriver(ctx context.Context, config *HDFSConfig) error {
	driver, err := NewHDFSORCDriver(ctx, config)
	if err != nil {
		return err
	}

	database.GetDriverRegistry().RegisterDriver(model.DatabaseTypeHDFSORC, driver)
	return nil
}
