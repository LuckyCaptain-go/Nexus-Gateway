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

// HudiDriver implements the Driver interface for Apache Hudi tables
type HudiDriver struct {
	restClient *HudiRESTClient
	parser     *HudiMetadataParser
	config     *HudiConfig
}

// NewHudiDriver creates a new Hudi driver
func NewHudiDriver(config *HudiConfig) (*HudiDriver, error) {
	restClient, err := NewHudiRESTClient(config)
	if err != nil {
		return nil, err
	}

	return &HudiDriver{
		restClient: restClient,
		parser:     NewHudiMetadataParser(),
		config:     config,
	}, nil
}

// Open opens a connection (not applicable for Hudi which uses compute engines)
func (d *HudiDriver) Open(dsn string) (*sql.DB, error) {
	return nil, fmt.Errorf("Hudi does not support direct database connections - use Spark, Flink, or REST API")
}

// ValidateDSN validates the connection string
func (d *HudiDriver) ValidateDSN(dsn string) error {
	if dsn == "" {
		return fmt.Errorf("DSN cannot be empty")
	}
	return nil
}

// GetDefaultPort returns the default port
func (d *HudiDriver) GetDefaultPort() int {
	return 8082 // Hudi REST service default port
}

// BuildDSN builds a connection string from configuration
func (d *HudiDriver) BuildDSN(config *model.DataSourceConfig) string {
	return fmt.Sprintf("hudi://%s:%d/%s", config.Host, config.Port, config.Database)
}

// GetDatabaseTypeName returns the database type name
func (d *HudiDriver) GetDatabaseTypeName() string {
	return "hudi"
}

// TestConnection tests the connection
func (d *HudiDriver) TestConnection(db *sql.DB) error {
	return fmt.Errorf("use TestConnectionREST instead for Hudi")
}

// TestConnectionREST tests REST API connectivity
func (d *HudiDriver) TestConnectionREST(ctx context.Context) error {
	// Try to get metadata as a connectivity test
	_, err := d.restClient.GetTableMetadata(ctx, "")
	return err
}

// GetDriverName returns the driver name
func (d *HudiDriver) GetDriverName() string {
	return "hudi-rest"
}

// GetCategory returns the driver category
func (d *HudiDriver) GetCategory() database.DriverCategory {
	return database.CategoryTableFormat
}

// GetCapabilities returns driver capabilities
func (d *HudiDriver) GetCapabilities() database.DriverCapabilities {
	return database.DriverCapabilities{
		SupportsSQL:             true, // Via compute engine
		SupportsTransaction:     true, // ACID support
		SupportsSchemaDiscovery: true,
		SupportsTimeTravel:      true,
		RequiresTokenRotation:   false,
		SupportsStreaming:       true,
	}
}

// ConfigureAuth configures authentication
func (d *HudiDriver) ConfigureAuth(authConfig interface{}) error {
	return nil
}

// =============================================================================
// Apache Hudi-Specific Methods
// =============================================================================

// ListPartitions lists all partitions for a table
func (d *HudiDriver) ListPartitions(ctx context.Context, basePath string) ([]HudiPartition, error) {
	return d.restClient.ListPartitions(ctx, basePath)
}

// GetTableMetadata retrieves table metadata
func (d *HudiDriver) GetTableMetadata(ctx context.Context, basePath string) (*HudiTableMetadata, error) {
	return d.restClient.GetTableMetadata(ctx, basePath)
}

// GetTableSchema retrieves table schema in standard format
func (d *HudiDriver) GetTableSchema(ctx context.Context, basePath, tableName string) (*metadata.TableSchema, error) {
	metadata, err := d.restClient.GetTableMetadata(ctx, basePath)
	if err != nil {
		return nil, err
	}

	parsedSchema, err := d.parser.ParseTableMetadata(metadata, tableName)
	if err != nil {
		return nil, err
	}

	return parsedSchema.Tables[tableName], nil
}

// QueryTable executes a SQL query
func (d *HudiDriver) QueryTable(ctx context.Context, sql string) (*HudiQueryResult, error) {
	return d.restClient.QueryTable(ctx, sql)
}

// GetCommits retrieves commit timeline
func (d *HudiDriver) GetCommits(ctx context.Context, basePath string) ([]HudiCommit, error) {
	return d.restClient.GetCommits(ctx, basePath)
}

// GetSavepoints retrieves savepoints
func (d *HudiDriver) GetSavepoints(ctx context.Context, basePath string) ([]HudiSavepoint, error) {
	return d.restClient.GetSavepoints(ctx, basePath)
}

// GetStatistics retrieves table statistics
func (d *HudiDriver) GetStatistics(ctx context.Context, basePath string) (*HudiTableStatistics, error) {
	commits, err := d.restClient.GetCommits(ctx, basePath)
	if err != nil {
		return nil, err
	}

	partitions, err := d.restClient.ListPartitions(ctx, basePath)
	if err != nil {
		return nil, err
	}

	recordStats := d.parser.CalculateRecordStats(commits)
	partitionStats := d.parser.GetPartitionStats(partitions)

	return &HudiTableStatistics{
		RecordStats:    recordStats,
		PartitionStats: partitionStats,
		CommitCount:    len(commits),
	}, nil
}

// RegisterHudiDriver registers the Hudi driver globally
func RegisterHudiDriver(config *HudiConfig) error {
	driver, err := NewHudiDriver(config)
	if err != nil {
		return err
	}

	database.GetDriverRegistry().RegisterDriver(model.DatabaseTypeApacheHudi, driver)
	return nil
}

// HudiTableStatistics contains table-level statistics
type HudiTableStatistics struct {
	RecordStats    *HudiRecordStats
	PartitionStats *PartitionStats
	CommitCount    int
}

// GetTableType determines if table is COW or MOR
func (d *HudiDriver) GetTableType(ctx context.Context, basePath string) (string, error) {
	metadata, err := d.restClient.GetTableMetadata(ctx, basePath)
	if err != nil {
		return "", err
	}

	return d.parser.GetTableType(metadata), nil
}

// GetCommitAtTime retrieves commit for a specific timestamp
func (d *HudiDriver) GetCommitAtTime(ctx context.Context, basePath string, timestamp time.Time) (*HudiCommit, error) {
	commits, err := d.restClient.GetCommits(ctx, basePath)
	if err != nil {
		return nil, err
	}

	return d.parser.GetCommitAtTime(commits, timestamp)
}

// GetLatestCommit retrieves the latest commit
func (d *HudiDriver) GetLatestCommit(ctx context.Context, basePath string) (*HudiCommit, error) {
	commits, err := d.restClient.GetCommits(ctx, basePath)
	if err != nil {
		return nil, err
	}

	return d.parser.GetLatestCommit(commits)
}

// GetCommitTimeline retrieves commit timeline
func (d *HudiDriver) GetCommitTimeline(ctx context.Context, basePath string) ([]CommitTimelineEntry, error) {
	commits, err := d.restClient.GetCommits(ctx, basePath)
	if err != nil {
		return nil, err
	}

	return d.parser.GetCommitTimeline(commits), nil
}

// SupportsIncrementalQuery checks if incremental queries are supported
func (d *HudiDriver) SupportsIncrementalQuery(ctx context.Context, basePath string) (bool, error) {
	metadata, err := d.restClient.GetTableMetadata(ctx, basePath)
	if err != nil {
		return false, err
	}

	return d.parser.SupportsIncrementalQuery(metadata), nil
}

// GetRecordStats calculates record statistics
func (d *HudiDriver) GetRecordStats(ctx context.Context, basePath string) (*HudiRecordStats, error) {
	commits, err := d.restClient.GetCommits(ctx, basePath)
	if err != nil {
		return nil, err
	}

	return d.parser.CalculateRecordStats(commits), nil
}

// GetPartitionSpec returns partition specification
func (d *HudiDriver) GetPartitionSpec(ctx context.Context, basePath string) ([]string, error) {
	metadata, err := d.restClient.GetTableMetadata(ctx, basePath)
	if err != nil {
		return nil, err
	}

	return d.parser.GetPartitionSpec(metadata), nil
}

// GetOperationTimeline retrieves operation timeline
func (d *HudiDriver) GetOperationTimeline(ctx context.Context, basePath string) ([]OperationEvent, error) {
	commits, err := d.restClient.GetCommits(ctx, basePath)
	if err != nil {
		return nil, err
	}

	return d.parser.GetOperationTimeline(commits), nil
}

// RollbackToInstant rolls back to a specific instant
func (d *HudiDriver) RollbackToInstant(ctx context.Context, basePath, instantTime string) error {
	// Hudi rollback requires executing rollback action
	// This would typically be done through Spark or REST API
	return fmt.Errorf("rollback not yet implemented")
}

// Savepoint creates a savepoint
func (d *HudiDriver) Savepoint(ctx context.Context, basePath, instantTime, user string) error {
	// Savepoint creation requires executing savepoint action
	return fmt.Errorf("savepoint creation not yet implemented")
}

// RestoreToSavepoint restores to a savepoint
func (d *HudiDriver) RestoreToSavepoint(ctx context.Context, basePath, savepointTime string) error {
	// Restore requires executing restore action
	return fmt.Errorf("restore to savepoint not yet implemented")
}

// CompactTable performs compaction (for MOR tables)
func (d *HudiDriver) CompactTable(ctx context.Context, basePath string) error {
	return fmt.Errorf("compaction not yet implemented")
}

// ClusterTable performs clustering
func (d *HudiDriver) ClusterTable(ctx context.Context, basePath string) error {
	return fmt.Errorf("clustering not yet implemented")
}

// GetCommitInfo retrieves detailed commit information
func (d *HudiDriver) GetCommitInfo(ctx context.Context, basePath, commitTime string) (*HudiCommit, error) {
	commits, err := d.restClient.GetCommits(ctx, basePath)
	if err != nil {
		return nil, err
	}

	for _, commit := range commits {
		if commit.CommitTime == commitTime {
			return &commit, nil
		}
	}

	return nil, fmt.Errorf("commit not found: %s", commitTime)
}

// GetSavepointInfo retrieves savepoint information
func (d *HudiDriver) GetSavepointInfo(ctx context.Context, basePath, savepointTime string) (*HudiSavepoint, error) {
	savepoints, err := d.restClient.GetSavepoints(ctx, basePath)
	if err != nil {
		return nil, err
	}

	for _, sp := range savepoints {
		if sp.SavepointTime == savepointTime {
			return &sp, nil
		}
	}

	return nil, fmt.Errorf("savepoint not found: %s", savepointTime)
}

// ValidateTableConfig validates Hudi table configuration
func (d *HudiDriver) ValidateTableConfig(config map[string]string) error {
	return d.parser.ValidateHudiTableConfig(config)
}

// GetCompressionCodec returns the compression codec
func (d *HudiDriver) GetCompressionCodec(ctx context.Context, basePath string) (string, error) {
	metadata, err := d.restClient.GetTableMetadata(ctx, basePath)
	if err != nil {
		return "", err
	}

	return d.parser.GetCompressionCodec(metadata), nil
}
