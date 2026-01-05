package file_system

import (
	"context"
	"database/sql"
	"fmt"
	"nexus-gateway/internal/database/drivers"

	"nexus-gateway/internal/database"
	"nexus-gateway/internal/model"
)

// HDFSHudiDriver implements Driver interface for HDFS Hudi tables
type HDFSHudiDriver struct {
	config *HDFSConfig
	client *HDFSClient
}

// NewHDFSHudiDriver creates a new HDFS Hudi driver
func NewHDFSHudiDriver(ctx context.Context, config *HDFSConfig) (*HDFSHudiDriver, error) {
	client, err := NewHDFSClient(ctx, config)
	if err != nil {
		return nil, err
	}

	return &HDFSHudiDriver{
		config: config,
		client: client,
	}, nil
}

// Open opens a connection (not applicable for HDFS Hudi)
func (d *HDFSHudiDriver) Open(dsn string) (*sql.DB, error) {
	return nil, fmt.Errorf("HDFS Hudi driver does not support standard database connections")
}

// ValidateDSN validates the connection string
func (d *HDFSHudiDriver) ValidateDSN(dsn string) error {
	if dsn == "" {
		return fmt.Errorf("DSN cannot be empty")
	}
	return nil
}

// GetDefaultPort returns the default HDFS port
func (d *HDFSHudiDriver) GetDefaultPort() int {
	return 8020
}

// BuildDSN builds a connection string from configuration
func (d *HDFSHudiDriver) BuildDSN(config *model.DataSourceConfig) string {
	return fmt.Sprintf("hdfs://%s:8020", config.Host)
}

// GetDatabaseTypeName returns the database type name
func (d *HDFSHudiDriver) GetDatabaseTypeName() string {
	return "hdfs-hudi"
}

// TestConnection tests if the connection is working
func (d *HDFSHudiDriver) TestConnection(db *sql.DB) error {
	return fmt.Errorf("use TestConnectionContext instead")
}

// TestConnectionContext tests the HDFS connection
func (d *HDFSHudiDriver) TestConnectionContext(ctx context.Context) error {
	_, err := d.client.GetCapacity(ctx)
	if err != nil {
		return fmt.Errorf("failed to test HDFS connection: %w", err)
	}
	return nil
}

// GetDriverName returns the driver name
func (d *HDFSHudiDriver) GetDriverName() string {
	return "hdfs-hudi"
}

// GetCategory returns the driver category
func (d *HDFSHudiDriver) GetCategory() drivers.DriverCategory {
	return drivers.CategoryFileSystem
}

// GetCapabilities returns driver capabilities
func (d *HDFSHudiDriver) GetCapabilities() drivers.DriverCapabilities {
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
func (d *HDFSHudiDriver) ConfigureAuth(authConfig interface{}) error {
	return nil
}

// GetTableMetadata retrieves Hudi table metadata
func (d *HDFSHudiDriver) GetTableMetadata(ctx context.Context, tablePath string) (*HudiMetadata, error) {
	// Hudi metadata is stored in .hoodie directory
	hoodiePath := tablePath + "/.hoodie"
	data, err := d.client.ReadFile(ctx, hoodiePath+"/hoodie.properties")
	if err != nil {
		return nil, fmt.Errorf("failed to read hoodie properties: %w", err)
	}

	// Parse Hudi properties (placeholder)
	return &HudiMetadata{
		TableLocation: tablePath,
		Format:        "hudi",
		TableType:     "COPY_ON_WRITE", // or MERGE_ON_READ
	}, nil
}

// HudiMetadata and HudiQueryResult are defined in common_types.go
func (d *HDFSHudiDriver) QueryTable(ctx context.Context, tablePath string, instantTime string) (*HudiQueryResult, error) {
	// Query Hudi table (placeholder)
	return &HudiQueryResult{
		Rows:        []map[string]interface{}{},
		NumRows:     0,
		InstantTime: instantTime,
	}, nil
}

// RegisterHDFSHudiDriver registers the HDFS Hudi driver globally
func RegisterHDFSHudiDriver(ctx context.Context, config *HDFSConfig) error {
	driver, err := NewHDFSHudiDriver(ctx, config)
	if err != nil {
		return err
	}

	database.GetDriverRegistry().RegisterDriver(model.DatabaseTypeHDFSHudi, driver)
	return nil
}
