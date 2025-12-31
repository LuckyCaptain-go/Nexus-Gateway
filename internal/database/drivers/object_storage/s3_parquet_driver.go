package object_storage

import (
	"context"
	"database/sql"
	"fmt"
	"io"

	"nexus-gateway/internal/database"
	"nexus-gateway/internal/model"
)

// S3ParquetDriver implements Driver interface for querying Parquet files on S3
type S3ParquetDriver struct {
	s3Client      *S3Client
	parquetReader *ParquetReader
	selectHandler *S3SelectHandler
	config        *S3ParquetDriverConfig
}

// S3ParquetDriverConfig holds S3 Parquet driver configuration
type S3ParquetDriverConfig struct {
	S3Config        *S3Config
	BatchSize       int
	EnableSelectAPI bool
	EnablePushdown  bool
}

// NewS3ParquetDriver creates a new S3 Parquet driver
func NewS3ParquetDriver(ctx context.Context, config *S3ParquetDriverConfig) (*S3ParquetDriver, error) {
	s3Client, err := NewS3Client(ctx, config.S3Config)
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 client: %w", err)
	}

	parquetConfig := &ParquetReaderConfig{
		BatchSize: config.BatchSize,
		Pushdown:  config.EnablePushdown,
	}

	parquetReader := NewParquetReader(s3Client, parquetConfig)

	selectHandler := NewS3SelectHandler(s3Client)

	return &S3ParquetDriver{
		s3Client:      s3Client,
		parquetReader: parquetReader,
		selectHandler: selectHandler,
		config:        config,
	}, nil
}

// Open opens a connection (not applicable for S3 Parquet)
func (d *S3ParquetDriver) Open(dsn string) (*sql.DB, error) {
	return nil, fmt.Errorf("S3 Parquet driver does not support standard database connections")
}

// ValidateDSN validates the connection string
func (d *S3ParquetDriver) ValidateDSN(dsn string) error {
	// Parse S3 URI
	bucket, key, err := ParseS3URI(dsn)
	if err != nil {
		return fmt.Errorf("invalid S3 URI: %w", err)
	}

	if bucket == "" {
		return fmt.Errorf("bucket is required in S3 URI")
	}

	return nil
}

// GetDefaultPort returns the default S3 port
func (d *S3ParquetDriver) GetDefaultPort() int {
	return 443 // HTTPS
}

// BuildDSN builds a connection string from configuration
func (d *S3ParquetDriver) BuildDSN(config *model.DataSourceConfig) string {
	return fmt.Sprintf("s3://%s/%s", config.Database, config.Host)
}

// GetDatabaseTypeName returns the database type name
func (d *S3ParquetDriver) GetDatabaseTypeName() string {
	return "s3-parquet"
}

// TestConnection tests if the connection is working
func (d *S3ParquetDriver) TestConnection(db *sql.DB) error {
	return fmt.Errorf("use TestConnectionContext instead")
}

// TestConnectionContext tests the S3 connection
func (d *S3ParquetDriver) TestConnectionContext(ctx context.Context) error {
	// Try to list objects in bucket
	_, err := d.s3Client.ListObjects(ctx, "", 1)
	if err != nil {
		return fmt.Errorf("failed to list S3 objects: %w", err)
	}
	return nil
}

// GetDriverName returns the driver name
func (d *S3ParquetDriver) GetDriverName() string {
	return "s3-parquet"
}

// GetCategory returns the driver category
func (d *S3ParquetDriver) GetCategory() database.DriverCategory {
	return database.CategoryObjectStorage
}

// GetCapabilities returns driver capabilities
func (d *S3ParquetDriver) GetCapabilities() database.DriverCapabilities {
	return database.DriverCapabilities{
		SupportsSQL:             false, // Uses S3 Select SQL dialect
		SupportsTransaction:     false,
		SupportsSchemaDiscovery: true,
		SupportsTimeTravel:      false,
		RequiresTokenRotation:   false,
		SupportsStreaming:       true,
	}
}

// ConfigureAuth configures authentication
func (d *S3ParquetDriver) ConfigureAuth(authConfig interface{}) error {
	return nil
}

// =============================================================================
// S3 Parquet-Specific Methods
// =============================================================================

// Query executes a query against Parquet files
func (d *S3ParquetDriver) Query(ctx context.Context, query *S3ParquetQuery) (*S3ParquetResult, error) {
	if query.Key == "" {
		return nil, fmt.Errorf("Parquet file key is required")
	}

	// Use S3 Select if enabled and applicable
	if d.config.EnableSelectAPI && query.UseSelectAPI {
		return d.queryWithSelect(ctx, query)
	}

	// Otherwise use Parquet reader
	return d.queryWithReader(ctx, query)
}

// S3ParquetQuery represents a query against S3 Parquet files
type S3ParquetQuery struct {
	Key          string
	Columns      []string
	Filters      map[string]interface{}
	Limit        int
	Offset       int
	UseSelectAPI bool
	SelectSQL    string // Custom SQL for S3 Select
}

// S3ParquetResult represents query results
type S3ParquetResult struct {
	Rows      []map[string]interface{}
	Schema    *ParquetSchema
	Metadata  *ParquetMetadata
	BytesRead int64
	NumRows   int64
}

// queryWithSelect uses S3 Select API
func (d *S3ParquetDriver) queryWithSelect(ctx context.Context, query *S3ParquetQuery) (*S3ParquetResult, error) {
	sql := query.SelectSQL
	if sql == "" {
		sql = BuildCSVQuery(query.Key, "", query.Columns)
	}

	result, err := d.selectHandler.QueryParquet(ctx, query.Key, sql)
	if err != nil {
		return nil, fmt.Errorf("S3 Select query failed: %w", err)
	}

	return &S3ParquetResult{
		Rows:      result.Records,
		NumRows:   int64(len(result.Records)),
		BytesRead: result.BytesProcessed,
	}, nil
}

// queryWithReader uses Parquet reader
func (d *S3ParquetDriver) queryWithReader(ctx context.Context, query *S3ParquetQuery) (*S3ParquetResult, error) {
	// Configure reader with column selection
	readerConfig := &ParquetReaderConfig{
		BatchSize: d.config.BatchSize,
		Pushdown:  d.config.EnablePushdown,
		Columns:   query.Columns,
	}

	reader := NewParquetReader(d.s3Client, readerConfig)
	fileReader, err := reader.Read(ctx, query.Key)
	if err != nil {
		return nil, fmt.Errorf("failed to open Parquet file: %w", err)
	}
	defer fileReader.Close()

	// Read all rows
	rows, err := fileReader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to read Parquet file: %w", err)
	}

	// Apply filters if specified
	if len(query.Filters) > 0 {
		rows = d.filterRows(rows, query.Filters)
	}

	// Apply offset and limit
	if query.Offset > 0 {
		if query.Offset >= len(rows) {
			rows = []map[string]interface{}{}
		} else {
			rows = rows[query.Offset:]
		}
	}

	if query.Limit > 0 && query.Limit < len(rows) {
		rows = rows[:query.Limit]
	}

	// Get metadata
	metadata, err := reader.GetMetadata(ctx, query.Key)
	if err != nil {
		metadata = nil
	}

	// Get schema
	schema, err := fileReader.GetSchema()
	if err != nil {
		schema = nil
	}

	return &S3ParquetResult{
		Rows:     rows,
		Schema:   schema,
		Metadata: metadata,
		NumRows:  int64(len(rows)),
	}, nil
}

// filterRows applies filters to rows
func (d *S3ParquetDriver) filterRows(rows []map[string]interface{}, filters map[string]interface{}) []map[string]interface{} {
	filtered := make([]map[string]interface{}, 0, len(rows))

	for _, row := range rows {
		if d.matchesFilters(row, filters) {
			filtered = append(filtered, row)
		}
	}

	return filtered
}

// matchesFilters checks if a row matches all filters
func (d *S3ParquetDriver) matchesFilters(row map[string]interface{}, filters map[string]interface{}) bool {
	for col, expectedVal := range filters {
		actualVal, exists := row[col]
		if !exists || actualVal != expectedVal {
			return false
		}
	}
	return true
}

// ListParquetFiles lists Parquet files in a prefix
func (d *S3ParquetDriver) ListParquetFiles(ctx context.Context, prefix string) ([]S3Object, error) {
	return d.s3Client.ListFilesByExtension(ctx, prefix, ".parquet")
}

// GetFileMetadata retrieves metadata for a Parquet file
func (d *S3ParquetDriver) GetFileMetadata(ctx context.Context, key string) (*ParquetMetadata, error) {
	return d.parquetReader.GetMetadata(ctx, key)
}

// GetSchema retrieves schema for a Parquet file
func (d *S3ParquetDriver) GetSchema(ctx context.Context, key string) (*ParquetSchema, error) {
	reader, err := d.parquetReader.Read(ctx, key)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return reader.GetSchema()
}

// GetColumnNames returns column names for a Parquet file
func (d *S3ParquetDriver) GetColumnNames(ctx context.Context, key string) ([]string, error) {
	return d.parquetReader.GetColumnNames(ctx, key)
}

// ReadColumns reads specific columns from a Parquet file
func (d *S3ParquetDriver) ReadColumns(ctx context.Context, key string, columns []string) ([]map[string]interface{}, error) {
	return d.parquetReader.ReadColumns(ctx, key, columns)
}

// QueryWithPredicate queries with predicate pushdown
func (d *S3ParquetDriver) QueryWithPredicate(ctx context.Context, key string, filter *ParquetFilter) ([]map[string]interface{}, error) {
	return d.parquetReader.FilterParquetFile(ctx, key, filter)
}

// StreamQuery streams query results
func (d *S3ParquetDriver) StreamQuery(ctx context.Context, key string, callback func(map[string]interface{}) error) error {
	reader, err := d.parquetReader.Read(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to open Parquet file: %w", err)
	}
	defer reader.Close()

	iterator := NewParquetRowIterator(reader, d.config.BatchSize)
	defer iterator.Close()

	for {
		row, err := iterator.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		if err := callback(row); err != nil {
			return err
		}
	}

	return nil
}

// GetFileCount counts Parquet files in a prefix
func (d *S3ParquetDriver) GetFileCount(ctx context.Context, prefix string) (int, error) {
	files, err := d.ListParquetFiles(ctx, prefix)
	if err != nil {
		return 0, err
	}
	return len(files), nil
}

// GetTotalSize calculates total size of Parquet files in a prefix
func (d *S3ParquetDriver) GetTotalSize(ctx context.Context, prefix string) (int64, error) {
	files, err := d.ListParquetFiles(ctx, prefix)
	if err != nil {
		return 0, err
	}

	var total int64
	for _, file := range files {
		total += file.Size
	}

	return total, nil
}

// ScanPartition scans a partition and returns metadata
func (d *S3ParquetDriver) ScanPartition(ctx context.Context, prefix string) (*PartitionMetadata, error) {
	files, err := d.ListParquetFiles(ctx, prefix)
	if err != nil {
		return nil, err
	}

	partition := &PartitionMetadata{
		Prefix:    prefix,
		FileCount: len(files),
		Files:     make([]FileMetadata, 0, len(files)),
	}

	var totalSize int64
	for _, file := range files {
		metadata, err := d.GetFileMetadata(ctx, file.Key)
		if err != nil {
			continue
		}

		partition.Files = append(partition.Files, FileMetadata{
			Key:     file.Key,
			Size:    file.Size,
			NumRows: metadata.NumRows,
		})

		totalSize += file.Size
		partition.TotalRows += metadata.NumRows
	}

	partition.TotalSize = totalSize

	return partition, nil
}

// PartitionMetadata represents partition metadata
type PartitionMetadata struct {
	Prefix    string
	FileCount int
	TotalRows int64
	TotalSize int64
	Files     []FileMetadata
}

// FileMetadata represents file metadata
type FileMetadata struct {
	Key     string
	Size    int64
	NumRows int64
}

// BatchQuery queries multiple Parquet files in batch
func (d *S3ParquetDriver) BatchQuery(ctx context.Context, keys []string, query *S3ParquetQuery) ([]*S3ParquetResult, error) {
	results := make([]*S3ParquetResult, len(keys))

	for i, key := range keys {
		query.Key = key
		result, err := d.Query(ctx, query)
		if err != nil {
			return nil, fmt.Errorf("query failed for %s: %w", key, err)
		}
		results[i] = result
	}

	return results, nil
}

// ValidateFile checks if a Parquet file is valid
func (d *S3ParquetDriver) ValidateFile(ctx context.Context, key string) error {
	reader, err := d.parquetReader.Read(ctx, key)
	if err != nil {
		return err
	}
	defer reader.Close()

	// Try to read schema
	_, err = reader.GetSchema()
	return err
}

// GetFileStatistics retrieves statistics for a Parquet file
func (d *S3ParquetDriver) GetFileStatistics(ctx context.Context, key string) (*FileStatistics, error) {
	metadata, err := d.GetFileMetadata(ctx, key)
	if err != nil {
		return nil, err
	}

	// Get object metadata for file size
	objMetadata, err := d.s3Client.GetObjectMetadata(ctx, key)
	if err != nil {
		return nil, err
	}

	return &FileStatistics{
		Key:       key,
		NumRows:   metadata.NumRows,
		FileSize:  objMetadata.ContentLength,
		RowGroups: len(metadata.RowGroups),
		Columns:   len(metadata.Schema.Columns),
	}, nil
}

// FileStatistics represents file statistics
type FileStatistics struct {
	Key       string
	NumRows   int64
	FileSize  int64
	RowGroups int
	Columns   int
}

// RegisterS3ParquetDriver registers the S3 Parquet driver globally
func RegisterS3ParquetDriver(ctx context.Context, config *S3ParquetDriverConfig) error {
	driver, err := NewS3ParquetDriver(ctx, config)
	if err != nil {
		return err
	}

	database.GetDriverRegistry().RegisterDriver(model.DatabaseTypeS3Parquet, driver)
	return nil
}

// ConvertToStandardSchema converts Parquet schema to standard schema
func (d *S3ParquetDriver) ConvertToStandardSchema(parquetSchema *ParquetSchema) model.TableSchema {
	stdSchema := model.TableSchema{
		Columns: make([]model.ColumnInfo, 0, len(parquetSchema.Columns)),
	}

	for _, col := range parquetSchema.Columns {
		stdCol := model.ColumnInfo{
			Name:     col.Name,
			Type:     mapParquetTypeToStandard(col.Type),
			Nullable: col.RepetitionType == "OPTIONAL",
		}

		if len(col.Path) > 0 {
			stdCol.NestedPath = col.Path
		}

		stdSchema.Columns = append(stdSchema.Columns, stdCol)
	}

	return stdSchema
}

// mapParquetTypeToStandard maps Parquet type to standard type
func mapParquetTypeToStandard(parquetType string) model.StandardizedType {
	switch parquetType {
	case "BOOLEAN":
		return model.StandardizedTypeBoolean
	case "INT32", "INT64":
		return model.StandardizedTypeInt64
	case "FLOAT", "DOUBLE":
		return model.StandardizedTypeFloat64
	case "BINARY", "FIXED_LEN_BYTE_ARRAY":
		return model.StandardizedTypeBinary
	case "UTF8":
		return model.StandardizedTypeString
	case "DATE":
		return model.StandardizedTypeDate
	case "TIMESTAMP", "TIMESTAMP_MILLIS", "TIMESTAMP_MICROS":
		return model.StandardizedTypeTimestamp
	case "LIST", "ARRAY":
		return model.StandardizedTypeArray
	case "MAP", "STRUCT":
		return model.StandardizedTypeStruct
	case "DECIMAL":
		return model.StandardizedTypeDecimal
	default:
		return model.StandardizedTypeString
	}
}
