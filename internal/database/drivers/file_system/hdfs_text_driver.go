package file_system

import (
	"context"
	"database/sql"
	"fmt"

	"nexus-gateway/internal/database"
	"nexus-gateway/internal/model"
)

// HDFSTextDriver implements Driver interface for HDFS text files
type HDFSTextDriver struct {
	config *HDFSConfig
	client *HDFSClient
}

// NewHDFSTextDriver creates a new HDFS text driver
func NewHDFSTextDriver(ctx context.Context, config *HDFSConfig) (*HDFSTextDriver, error) {
	client, err := NewHDFSClient(ctx, config)
	if err != nil {
		return nil, err
	}

	return &HDFSTextDriver{
		config: config,
		client: client,
	}, nil
}

// Open opens a connection (not applicable for HDFS text)
func (d *HDFSTextDriver) Open(dsn string) (*sql.DB, error) {
	return nil, fmt.Errorf("HDFS text driver does not support standard database connections")
}

// ValidateDSN validates the connection string
func (d *HDFSTextDriver) ValidateDSN(dsn string) error {
	if dsn == "" {
		return fmt.Errorf("DSN cannot be empty")
	}
	return nil
}

// GetDefaultPort returns the default HDFS port
func (d *HDFSTextDriver) GetDefaultPort() int {
	return 8020
}

// BuildDSN builds a connection string from configuration
func (d *HDFSTextDriver) BuildDSN(config *model.DataSourceConfig) string {
	return fmt.Sprintf("hdfs://%s:8020", config.Host)
}

// GetDatabaseTypeName returns the database type name
func (d *HDFSTextDriver) GetDatabaseTypeName() string {
	return "hdfs-text"
}

// TestConnection tests if the connection is working
func (d *HDFSTextDriver) TestConnection(db *sql.DB) error {
	return fmt.Errorf("use TestConnectionContext instead")
}

// TestConnectionContext tests the HDFS connection
func (d *HDFSTextDriver) TestConnectionContext(ctx context.Context) error {
	_, err := d.client.GetCapacity(ctx)
	if err != nil {
		return fmt.Errorf("failed to test HDFS connection: %w", err)
	}
	return nil
}

// GetDriverName returns the driver name
func (d *HDFSTextDriver) GetDriverName() string {
	return "hdfs-text"
}

// GetCategory returns the driver category
func (d *HDFSTextDriver) GetCategory() database.DriverCategory {
	return database.CategoryFileSystem
}

// GetCapabilities returns driver capabilities
func (d *HDFSTextDriver) GetCapabilities() database.DriverCapabilities {
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
func (d *HDFSTextDriver) ConfigureAuth(authConfig interface{}) error {
	return nil
}

// ListTextFiles lists text files in a directory
func (d *HDFSTextDriver) ListTextFiles(ctx context.Context, path string) ([]HDFSFileInfo, error) {
	files, err := d.client.ListFiles(ctx, path)
	if err != nil {
		return nil, err
	}

	var textFiles []HDFSFileInfo
	for _, file := range files {
		if !file.IsDir {
			if len(file.Name) > 4 && (file.Name[len(file.Name)-4:] == ".txt" || file.Name[len(file.Name)-4:] == ".log") {
				textFiles = append(textFiles, file)
			}
		}
	}

	return textFiles, nil
}

// QueryTextFile queries a text file from HDFS
func (d *HDFSTextDriver) QueryTextFile(ctx context.Context, path string) (*HDFSTextResult, error) {
	data, err := d.client.ReadFile(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("failed to read text file: %w", err)
	}

	// Parse text file (placeholder)
	return &HDFSTextResult{
		Rows:      []string{string(data)},
		LineCount: 1,
		BytesRead: int64(len(data)),
		FilePath:  path,
	}, nil
}

// HDFSTextResult represents query result
type HDFSTextResult struct {
	Rows      []string
	LineCount int
	BytesRead int64
	FilePath  string
}

// RegisterHDFSTextDriver registers the HDFS text driver globally
func RegisterHDFSTextDriver(ctx context.Context, config *HDFSConfig) error {
	driver, err := NewHDFSTextDriver(ctx, config)
	if err != nil {
		return err
	}

	database.GetDriverRegistry().RegisterDriver(model.DatabaseTypeHDFSText, driver)
	return nil
}
