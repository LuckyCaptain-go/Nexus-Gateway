package table_formats

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"nexus-gateway/internal/database"
	"nexus-gateway/internal/database/metadata"
	"nexus-gateway/internal/model"
)

// DeltaDriver implements the Driver interface for Delta Lake tables
type DeltaDriver struct {
	restClient *DeltaRESTClient
	parser     *DeltaLogParser
	config     *DeltaConfig
}

// NewDeltaDriver creates a new Delta Lake driver
func NewDeltaDriver(config *DeltaConfig) (*DeltaDriver, error) {
	restClient, err := NewDeltaRESTClient(config)
	if err != nil {
		return nil, err
	}

	return &DeltaDriver{
		restClient: restClient,
		parser:     NewDeltaLogParser(),
		config:     config,
	}, nil
}

// Open opens a connection (not applicable for Delta Lake which uses REST)
func (d *DeltaDriver) Open(dsn string) (*sql.DB, error) {
	return nil, fmt.Errorf("Delta Lake does not support direct database connections - use Databricks SQL Warehouse or REST API")
}

// ValidateDSN validates the connection string
func (d *DeltaDriver) ValidateDSN(dsn string) error {
	if dsn == "" {
		return fmt.Errorf("DSN cannot be empty")
	}
	return nil
}

// GetDefaultPort returns the default port
func (d *DeltaDriver) GetDefaultPort() int {
	return 443 // HTTPS
}

// BuildDSN builds a connection string from configuration
func (d *DeltaDriver) BuildDSN(config *model.DataSourceConfig) string {
	return fmt.Sprintf("delta://%s:%d/%s", config.Host, config.Port, config.Database)
}

// GetDatabaseTypeName returns the database type name
func (d *DeltaDriver) GetDatabaseTypeName() string {
	return "delta_lake"
}

// TestConnection tests the connection
func (d *DeltaDriver) TestConnection(db *sql.DB) error {
	return fmt.Errorf("use TestConnectionREST instead for Delta Lake")
}

// TestConnectionREST tests REST API connectivity
func (d *DeltaDriver) TestConnectionREST(ctx context.Context) error {
	// Try to list tables as a connectivity test
	_, err := d.restClient.ListTables(ctx, "", "")
	return err
}

// GetDriverName returns the driver name
func (d *DeltaDriver) GetDriverName() string {
	return "delta-lake-rest"
}

// GetCategory returns the driver category
func (d *DeltaDriver) GetCategory() database.DriverCategory {
	return database.CategoryTableFormat
}

// GetCapabilities returns driver capabilities
func (d *DeltaDriver) GetCapabilities() database.DriverCapabilities {
	return database.DriverCapabilities{
		SupportsSQL:             true, // Via Databricks SQL Warehouse
		SupportsTransaction:     true, // ACID support
		SupportsSchemaDiscovery: true,
		SupportsTimeTravel:      true,
		RequiresTokenRotation:   false,
		SupportsStreaming:       true,
	}
}

// ConfigureAuth configures authentication
func (d *DeltaDriver) ConfigureAuth(authConfig interface{}) error {
	return nil
}

// =============================================================================
// Delta Lake-Specific Methods
// =============================================================================

// ListTables lists all tables
func (d *DeltaDriver) ListTables(ctx context.Context, catalog, schema string) ([]DeltaTableIdentifier, error) {
	return d.restClient.ListTables(ctx, catalog, schema)
}

// GetTableDetails retrieves table details
func (d *DeltaDriver) GetTableDetails(ctx context.Context, catalog, schema, table string) (*DeltaTableDetails, error) {
	return d.restClient.GetTableDetails(ctx, catalog, schema, table)
}

// GetTableSchema retrieves table schema in standard format
func (d *DeltaDriver) GetTableSchema(ctx context.Context, catalog, schema, table string) (*metadata.TableSchema, error) {
	details, err := d.restClient.GetTableDetails(ctx, catalog, schema, table)
	if err != nil {
		return nil, err
	}

	fullName := fmt.Sprintf("%s.%s.%s", catalog, schema, table)
	parsedSchema, err := d.parser.ParseTableMetadata(details, fullName)
	if err != nil {
		return nil, err
	}

	return parsedSchema.Tables[fullName], nil
}

// QueryTable executes a SQL query via Databricks SQL Warehouse
func (d *DeltaDriver) QueryTable(ctx context.Context, warehouseID, sql string) (*DeltaQueryResult, error) {
	return d.restClient.QueryTable(ctx, warehouseID, sql)
}

// GetVersionAtTime retrieves version for a specific timestamp
func (d *DeltaDriver) GetVersionAtTime(ctx context.Context, tablePath string, timestamp time.Time) (int64, error) {
	commits, err := d.restClient.GetTableHistory(ctx, tablePath)
	if err != nil {
		return 0, err
	}

	return d.parser.GetCommitAtTime(commits, timestamp)
}

// GetStatistics retrieves table statistics
func (d *DeltaDriver) GetStatistics(ctx context.Context, catalog, schema, table string) (*DeltaTableStatistics, error) {
	details, err := d.restClient.GetTableDetails(ctx, catalog, schema, table)
	if err != nil {
		return nil, err
	}

	stats := &DeltaTableStatistics{
		CreatedAt:       time.UnixMilli(details.CreatedAt),
		CreatedBy:       details.CreatedBy,
		StorageLocation: details.StorageLocation,
		ColumnCount:     len(details.Columns),
	}

	return stats, nil
}

// RegisterDeltaDriver registers the Delta Lake driver globally
func RegisterDeltaDriver(config *DeltaConfig) error {
	driver, err := NewDeltaDriver(config)
	if err != nil {
		return err
	}

	database.GetDriverRegistry().RegisterDriver(model.DatabaseTypeDeltaLake, driver)
	return nil
}

// DeltaTableStatistics contains table-level statistics
type DeltaTableStatistics struct {
	CreatedAt       time.Time
	CreatedBy       string
	StorageLocation string
	ColumnCount     int
	RowCount        int64 // Requires parsing stats
	TableSize       int64 // Requires parsing file actions
}

// GetPartitioningStrategy returns the partitioning strategy
func (d *DeltaDriver) GetPartitioningStrategy(ctx context.Context, catalog, schema, table string) ([]string, error) {
	details, err := d.restClient.GetTableDetails(ctx, catalog, schema, table)
	if err != nil {
		return nil, err
	}

	// Delta partition info would be in table properties
	// For now, return empty slice
	return []string{}, nil
}

// GetHistory retrieves table commit history
func (d *DeltaDriver) GetHistory(ctx context.Context, tablePath string) ([]DeltaCommitInfo, error) {
	return d.restClient.GetTableHistory(ctx, tablePath)
}

// GetVersionInfo retrieves information about a specific version
func (d *DeltaDriver) GetVersionInfo(ctx context.Context, tablePath string, version int64) (*DeltaCommitInfo, error) {
	commits, err := d.restClient.GetTableHistory(ctx, tablePath)
	if err != nil {
		return nil, err
	}

	for _, commit := range commits {
		if commit.Version == version {
			return &commit, nil
		}
	}

	return nil, fmt.Errorf("version %d not found", version)
}

// Vacuum runs vacuum operation to clean up old files
func (d *DeltaDriver) Vacuum(ctx context.Context, tablePath string, retentionHours float64, dryRun bool) (*VacuumResult, error) {
	// Vacuum would require executing SQL command
	// VACUUM table_name RETAIN num_hours DRY RUN
	// This is a placeholder implementation
	return &VacuumResult{
		FilesDeleted: 0,
		BytesDeleted: 0,
		DryRun:       dryRun,
	}, nil
}

// VacuumResult represents vacuum operation result
type VacuumResult struct {
	FilesDeleted int
	BytesDeleted int64
	DryRun       bool
}

// DescribeHistory returns detailed history of operations
func (d *DeltaDriver) DescribeHistory(ctx context.Context, tablePath string, limit int) ([]DeltaCommitInfo, error) {
	commits, err := d.restClient.GetTableHistory(ctx, tablePath)
	if err != nil {
		return nil, err
	}

	if limit > 0 && len(commits) > limit {
		start := len(commits) - limit
		if start < 0 {
			start = 0
		}
		commits = commits[start:]
	}

	return commits, nil
}

// GenerateManifest generates a table manifest
func (d *DeltaDriver) GenerateManifest(ctx context.Context, catalog, schema, table string) (string, error) {
	// Manifest generation would require specific Delta operations
	return "", fmt.Errorf("manifest generation not yet implemented")
}

// CloneTable clones a table (Delta Lake feature)
func (d *DeltaDriver) CloneTable(ctx context.Context, sourceTable, targetTable string, deep bool) error {
	return fmt.Errorf("table cloning not yet implemented")
}

// ConvertToDelta converts a table to Delta format
func (d *DeltaDriver) ConvertToDelta(ctx context.Context, sourceTable, format string) error {
	return fmt.Errorf("convert to delta not yet implemented")
}

// UpdateTableVersion updates table to a specific version
func (d *DeltaDriver) UpdateTableVersion(ctx context.Context, tablePath string, version int64) error {
	return fmt.Errorf("version restore not yet implemented")
}

// GetLatestVersion retrieves the latest version
func (d *DeltaDriver) GetLatestVersion(ctx context.Context, tablePath string) (int64, error) {
	commits, err := d.restClient.GetTableHistory(ctx, tablePath)
	if err != nil {
		return 0, err
	}

	return d.parser.GetLatestVersion(commits)
}
