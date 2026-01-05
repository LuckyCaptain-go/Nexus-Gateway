package file_system

import (
	"context"
	"database/sql"
	"fmt"
	"nexus-gateway/internal/database/drivers"

	"nexus-gateway/internal/database"
	"nexus-gateway/internal/model"
)

// HDFSJSONDriver implements Driver interface for HDFS JSON files
type HDFSJSONDriver struct {
	config *HDFSConfig
	client *HDFSClient
}

// NewHDFSJSONDriver creates a new HDFS JSON driver
func NewHDFSJSONDriver(ctx context.Context, config *HDFSConfig) (*HDFSJSONDriver, error) {
	client, err := NewHDFSClient(ctx, config)
	if err != nil {
		return nil, err
	}

	return &HDFSJSONDriver{
		config: config,
		client: client,
	}, nil
}

// Open opens a connection (not applicable for HDFS JSON)
func (d *HDFSJSONDriver) Open(dsn string) (*sql.DB, error) {
	return nil, fmt.Errorf("HDFS JSON driver does not support standard database connections")
}

// ValidateDSN validates the connection string
func (d *HDFSJSONDriver) ValidateDSN(dsn string) error {
	if dsn == "" {
		return fmt.Errorf("DSN cannot be empty")
	}
	return nil
}

// GetDefaultPort returns the default HDFS port
func (d *HDFSJSONDriver) GetDefaultPort() int {
	return 8020
}

// BuildDSN builds a connection string from configuration
func (d *HDFSJSONDriver) BuildDSN(config *model.DataSourceConfig) string {
	return fmt.Sprintf("hdfs://%s:8020", config.Host)
}

// GetDatabaseTypeName returns the database type name
func (d *HDFSJSONDriver) GetDatabaseTypeName() string {
	return "hdfs-json"
}

// TestConnection tests if the connection is working
func (d *HDFSJSONDriver) TestConnection(db *sql.DB) error {
	return fmt.Errorf("use TestConnectionContext instead")
}

// TestConnectionContext tests the HDFS connection
func (d *HDFSJSONDriver) TestConnectionContext(ctx context.Context) error {
	_, err := d.client.GetCapacity(ctx)
	if err != nil {
		return fmt.Errorf("failed to test HDFS connection: %w", err)
	}
	return nil
}

// GetDriverName returns the driver name
func (d *HDFSJSONDriver) GetDriverName() string {
	return "hdfs-json"
}

// GetCategory returns the driver category
func (d *HDFSJSONDriver) GetCategory() drivers.DriverCategory {
	return drivers.CategoryFileSystem
}

// GetCapabilities returns driver capabilities
func (d *HDFSJSONDriver) GetCapabilities() drivers.DriverCapabilities {
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
func (d *HDFSJSONDriver) ConfigureAuth(authConfig interface{}) error {
	return nil
}

// ListJSONFiles lists JSON files in a directory
func (d *HDFSJSONDriver) ListJSONFiles(ctx context.Context, path string) ([]HDFSFileInfo, error) {
	files, err := d.client.ListFiles(ctx, path)
	if err != nil {
		return nil, err
	}

	var jsonFiles []HDFSFileInfo
	for _, file := range files {
		if !file.IsDir {
			if len(file.Name) > 5 && file.Name[len(file.Name)-5:] == ".json" {
				jsonFiles = append(jsonFiles, file)
			} else if len(file.Name) > 9 && file.Name[len(file.Name)-9:] == ".jsonlines" {
				jsonFiles = append(jsonFiles, file)
			}
		}
	}

	return jsonFiles, nil
}

// QueryJSONFile queries a JSON file from HDFS
func (d *HDFSJSONDriver) QueryJSONFile(ctx context.Context, path string) (*HDFSJSONResult, error) {
	data, err := d.client.ReadFile(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("failed to read JSON file: %w", err)
	}

	// Parse JSON file (placeholder)
	return &HDFSJSONResult{
		Rows:      []map[string]interface{}{},
		NumRows:   0,
		BytesRead: int64(len(data)),
		FilePath:  path,
	}, nil
}

// HDFSJSONResult represents query result
type HDFSJSONResult struct {
	Rows      []map[string]interface{}
	NumRows   int64
	BytesRead int64
	FilePath  string
}

// RegisterHDFSJSONDriver registers the HDFS JSON driver globally
func RegisterHDFSJSONDriver(ctx context.Context, config *HDFSConfig) error {
	driver, err := NewHDFSJSONDriver(ctx, config)
	if err != nil {
		return err
	}

	database.GetDriverRegistry().RegisterDriver(model.DatabaseTypeHDFSJSON, driver)
	return nil
}
