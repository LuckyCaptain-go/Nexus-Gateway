package file_system

import (
	"context"
	"database/sql"
	"fmt"
	"nexus-gateway/internal/database/drivers"

	"nexus-gateway/internal/model"
)

// HDFSORCCompressionDriver implements Driver interface for HDFS compressed ORC files
type HDFSORCCompressionDriver struct {
	config *HDFSConfig
	client *HDFSClient
}

// NewHDFSORCCompressionDriver creates a new HDFS compressed ORC driver
func NewHDFSORCCompressionDriver(ctx context.Context, config *HDFSConfig) (*HDFSORCCompressionDriver, error) {
	client, err := NewHDFSClient(ctx, config)
	if err != nil {
		return nil, err
	}

	return &HDFSORCCompressionDriver{
		config: config,
		client: client,
	}, nil
}

// Open opens a connection (not applicable for HDFS compressed ORC)
func (d *HDFSORCCompressionDriver) Open(dsn string) (*sql.DB, error) {
	return nil, fmt.Errorf("HDFS compressed ORC driver does not support standard database connections")
}

// ValidateDSN validates the connection string
func (d *HDFSORCCompressionDriver) ValidateDSN(dsn string) error {
	if dsn == "" {
		return fmt.Errorf("DSN cannot be empty")
	}
	return nil
}

// GetDefaultPort returns the default HDFS port
func (d *HDFSORCCompressionDriver) GetDefaultPort() int {
	return 8020
}

// BuildDSN builds a connection string from configuration
func (d *HDFSORCCompressionDriver) BuildDSN(config *model.DataSourceConfig) string {
	return fmt.Sprintf("hdfs://%s:8020", config.Host)
}

// GetDatabaseTypeName returns the database type name
func (d *HDFSORCCompressionDriver) GetDatabaseTypeName() string {
	return "hdfs-orc-compressed"
}

// TestConnection tests if the connection is working
func (d *HDFSORCCompressionDriver) TestConnection(db *sql.DB) error {
	return fmt.Errorf("use TestConnectionContext instead")
}

// TestConnectionContext tests the HDFS connection
func (d *HDFSORCCompressionDriver) TestConnectionContext(ctx context.Context) error {
	_, err := d.client.GetCapacity(ctx)
	if err != nil {
		return fmt.Errorf("failed to test HDFS connection: %w", err)
	}
	return nil
}

// GetDriverName returns the driver name
func (d *HDFSORCCompressionDriver) GetDriverName() string {
	return "hdfs-orc-compressed"
}

// GetCategory returns the driver category
func (d *HDFSORCCompressionDriver) GetCategory() drivers.DriverCategory {
	return drivers.CategoryFileSystem
}

// GetCapabilities returns driver capabilities
func (d *HDFSORCCompressionDriver) GetCapabilities() drivers.DriverCapabilities {
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
func (d *HDFSORCCompressionDriver) ConfigureAuth(authConfig interface{}) error {
	return nil
}

// ListCompressedORCFiles lists compressed ORC files in a directory
func (d *HDFSORCCompressionDriver) ListCompressedORCFiles(ctx context.Context, path string) ([]HDFSFileInfo, error) {
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

// QueryCompressedORCFile queries a compressed ORC file from HDFS
func (d *HDFSORCCompressionDriver) QueryCompressedORCFile(ctx context.Context, path string) (*HDFSORCResult, error) {
	data, err := d.client.ReadFile(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("failed to read compressed ORC file: %w", err)
	}

	// Parse compressed ORC file (placeholder)
	return &HDFSORCResult{
		Rows:      []map[string]interface{}{},
		NumRows:   0,
		BytesRead: int64(len(data)),
		FilePath:  path,
	}, nil
}
