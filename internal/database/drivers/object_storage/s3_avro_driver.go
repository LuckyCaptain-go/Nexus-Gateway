package object_storage

import (
	"context"
	"database/sql"
	"fmt"

	"nexus-gateway/internal/database"
	"nexus-gateway/internal/model"
)

// S3AvroDriver implements Driver interface for querying Avro files on S3
type S3AvroDriver struct {
	s3Client   *S3Client
	avroReader *AvroReader
	config     *S3AvroDriverConfig
}

// S3AvroDriverConfig holds S3 Avro driver configuration
type S3AvroDriverConfig struct {
	S3Config  *S3Config
	BatchSize int
}

// NewS3AvroDriver creates a new S3 Avro driver
func NewS3AvroDriver(ctx context.Context, config *S3AvroDriverConfig) (*S3AvroDriver, error) {
	s3Client, err := NewS3Client(ctx, config.S3Config)
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 client: %w", err)
	}

	avroConfig := &AvroReaderConfig{
		BatchSize: config.BatchSize,
	}

	avroReader := NewAvroReader(s3Client, avroConfig)

	return &S3AvroDriver{
		s3Client:   s3Client,
		avroReader: avroReader,
		config:     config,
	}, nil
}

// Open opens a connection (not applicable for S3 Avro)
func (d *S3AvroDriver) Open(dsn string) (*sql.DB, error) {
	return nil, fmt.Errorf("S3 Avro driver does not support standard database connections")
}

// ValidateDSN validates the connection string
func (d *S3AvroDriver) ValidateDSN(dsn string) error {
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
func (d *S3AvroDriver) GetDefaultPort() int {
	return 443
}

// BuildDSN builds a connection string from configuration
func (d *S3AvroDriver) BuildDSN(config *model.DataSourceConfig) string {
	return fmt.Sprintf("s3://%s/%s", config.Database, config.Host)
}

// GetDatabaseTypeName returns the database type name
func (d *S3AvroDriver) GetDatabaseTypeName() string {
	return "s3-avro"
}

// TestConnection tests if the connection is working
func (d *S3AvroDriver) TestConnection(db *sql.DB) error {
	return fmt.Errorf("use TestConnectionContext instead")
}

// TestConnectionContext tests the S3 connection
func (d *S3AvroDriver) TestConnectionContext(ctx context.Context) error {
	_, err := d.s3Client.ListObjects(ctx, "", 1)
	if err != nil {
		return fmt.Errorf("failed to list S3 objects: %w", err)
	}
	return nil
}

// GetDriverName returns the driver name
func (d *S3AvroDriver) GetDriverName() string {
	return "s3-avro"
}

// GetCategory returns the driver category
func (d *S3AvroDriver) GetCategory() database.DriverCategory {
	return database.CategoryObjectStorage
}

// GetCapabilities returns driver capabilities
func (d *S3AvroDriver) GetCapabilities() database.DriverCapabilities {
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
func (d *S3AvroDriver) ConfigureAuth(authConfig interface{}) error {
	return nil
}

// Query executes a query against Avro files
func (d *S3AvroDriver) Query(ctx context.Context, key string) (*S3AvroResult, error) {
	reader, err := d.avroReader.Read(ctx, key)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	rows, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	return &S3AvroResult{
		Rows:    rows,
		NumRows: int64(len(rows)),
	}, nil
}

// S3AvroResult represents query results
type S3AvroResult struct {
	Rows    []map[string]interface{}
	NumRows int64
}

// GetSchema retrieves schema for an Avro file
func (d *S3AvroDriver) GetSchema(ctx context.Context, key string) (*AvroSchema, error) {
	reader, err := d.avroReader.Read(ctx, key)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return reader.GetSchema()
}

// ListAvroFiles lists Avro files in a prefix
func (d *S3AvroDriver) ListAvroFiles(ctx context.Context, prefix string) ([]S3Object, error) {
	return d.s3Client.ListFilesByExtension(ctx, prefix, ".avro")
}

// RegisterS3AvroDriver registers the S3 Avro driver globally
func RegisterS3AvroDriver(ctx context.Context, config *S3AvroDriverConfig) error {
	driver, err := NewS3AvroDriver(ctx, config)
	if err != nil {
		return err
	}

	database.GetDriverRegistry().RegisterDriver(model.DatabaseTypeS3Avro, driver)
	return nil
}
