package file_system

import (
	"context"
	"database/sql"
	"fmt"

	"nexus-gateway/internal/database"
	"nexus-gateway/internal/model"
)

// HDFSXMLDriver implements Driver interface for HDFS XML files
type HDFSXMLDriver struct {
	config *HDFSConfig
	client *HDFSClient
}

// NewHDFSXMLDriver creates a new HDFS XML driver
func NewHDFSXMLDriver(ctx context.Context, config *HDFSConfig) (*HDFSXMLDriver, error) {
	client, err := NewHDFSClient(ctx, config)
	if err != nil {
		return nil, err
	}

	return &HDFSXMLDriver{
		config: config,
		client: client,
	}, nil
}

// Open opens a connection (not applicable for HDFS XML)
func (d *HDFSXMLDriver) Open(dsn string) (*sql.DB, error) {
	return nil, fmt.Errorf("HDFS XML driver does not support standard database connections")
}

// ValidateDSN validates the connection string
func (d *HDFSXMLDriver) ValidateDSN(dsn string) error {
	if dsn == "" {
		return fmt.Errorf("DSN cannot be empty")
	}
	return nil
}

// GetDefaultPort returns the default HDFS port
func (d *HDFSXMLDriver) GetDefaultPort() int {
	return 8020
}

// BuildDSN builds a connection string from configuration
func (d *HDFSXMLDriver) BuildDSN(config *model.DataSourceConfig) string {
	return fmt.Sprintf("hdfs://%s:8020", config.Host)
}

// GetDatabaseTypeName returns the database type name
func (d *HDFSXMLDriver) GetDatabaseTypeName() string {
	return "hdfs-xml"
}

// TestConnection tests if the connection is working
func (d *HDFSXMLDriver) TestConnection(db *sql.DB) error {
	return fmt.Errorf("use TestConnectionContext instead")
}

// TestConnectionContext tests the HDFS connection
func (d *HDFSXMLDriver) TestConnectionContext(ctx context.Context) error {
	_, err := d.client.GetCapacity(ctx)
	if err != nil {
		return fmt.Errorf("failed to test HDFS connection: %w", err)
	}
	return nil
}

// GetDriverName returns the driver name
func (d *HDFSXMLDriver) GetDriverName() string {
	return "hdfs-xml"
}

// GetCategory returns the driver category
func (d *HDFSXMLDriver) GetCategory() database.DriverCategory {
	return database.CategoryFileSystem
}

// GetCapabilities returns driver capabilities
func (d *HDFSXMLDriver) GetCapabilities() database.DriverCapabilities {
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
func (d *HDFSXMLDriver) ConfigureAuth(authConfig interface{}) error {
	return nil
}

// ListXMLFiles lists XML files in a directory
func (d *HDFSXMLDriver) ListXMLFiles(ctx context.Context, path string) ([]HDFSFileInfo, error) {
	files, err := d.client.ListFiles(ctx, path)
	if err != nil {
		return nil, err
	}

	var xmlFiles []HDFSFileInfo
	for _, file := range files {
		if !file.IsDir && len(file.Name) > 4 && file.Name[len(file.Name)-4:] == ".xml" {
			xmlFiles = append(xmlFiles, file)
		}
	}

	return xmlFiles, nil
}

// QueryXMLFile queries an XML file from HDFS
func (d *HDFSXMLDriver) QueryXMLFile(ctx context.Context, path string, xpath string) (*HDFSXMLResult, error) {
	data, err := d.client.ReadFile(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("failed to read XML file: %w", err)
	}

	// Parse XML file (placeholder)
	return &HDFSXMLResult{
		Rows:     []map[string]interface{}{},
		NumRows:  0,
		BytesRead: int64(len(data)),
		FilePath: path,
		XPath:    xpath,
	}, nil
}

// HDFSXMLResult represents query result
type HDFSXMLResult struct {
	Rows      []map[string]interface{}
	NumRows   int64
	BytesRead int64
	FilePath  string
	XPath     string
}

// RegisterHDFSXMLDriver registers the HDFS XML driver globally
func RegisterHDFSXMLDriver(ctx context.Context, config *HDFSConfig) error {
	driver, err := NewHDFSXMLDriver(ctx, config)
	if err != nil {
		return err
	}

	database.GetDriverRegistry().RegisterDriver(model.DatabaseTypeHDFSXML, driver)
	return nil
}
