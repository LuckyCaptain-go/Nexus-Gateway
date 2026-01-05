package olap

import (
	"context"
	"database/sql"
	"fmt"
	"nexus-gateway/internal/database/drivers"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"nexus-gateway/internal/database"
	"nexus-gateway/internal/model"
)

// StarRocksDriver implements Driver interface for StarRocks
type StarRocksDriver struct {
	config *StarRocksConfig
}

// StarRocksConfig holds StarRocks configuration
type StarRocksConfig struct {
	Host           string
	Port           int
	Database       string
	Username       string
	Password       string
	EnablePipeline bool
}

// NewStarRocksDriver creates a new StarRocks driver
func NewStarRocksDriver(config *StarRocksConfig) (*StarRocksDriver, error) {
	if config.Host == "" {
		return nil, fmt.Errorf("host is required")
	}

	return &StarRocksDriver{
		config: config,
	}, nil
}

// Open opens a connection to StarRocks (MySQL-compatible)
func (d *StarRocksDriver) Open(dsn string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open StarRocks connection: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping StarRocks: %w", err)
	}

	return db, nil
}

// ValidateDSN validates the connection string
func (d *StarRocksDriver) ValidateDSN(dsn string) error {
	if dsn == "" {
		return fmt.Errorf("DSN cannot be empty")
	}
	return nil
}

// GetDefaultPort returns the default StarRocks port
func (d *StarRocksDriver) GetDefaultPort() int {
	return 9030
}

// BuildDSN builds a connection string from configuration
func (d *StarRocksDriver) BuildDSN(config *model.DataSourceConfig) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		config.Username, config.Password, config.Host, config.Port, config.Database)
}

// GetDatabaseTypeName returns the database type name
func (d *StarRocksDriver) GetDatabaseTypeName() string {
	return "starrocks"
}

// TestConnection tests if the connection is working
func (d *StarRocksDriver) TestConnection(db *sql.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return db.PingContext(ctx)
}

// GetDriverName returns the driver name
func (d *StarRocksDriver) GetDriverName() string {
	return "starrocks-mysql"
}

// GetCategory returns the driver category
func (d *StarRocksDriver) GetCategory() drivers.DriverCategory {
	return drivers.CategoryOLAP
}

// GetCapabilities returns driver capabilities
func (d *StarRocksDriver) GetCapabilities() drivers.DriverCapabilities {
	return drivers.DriverCapabilities{
		SupportsSQL:             true,
		SupportsTransaction:     true,
		SupportsSchemaDiscovery: true,
		SupportsTimeTravel:      false,
		RequiresTokenRotation:   false,
		SupportsStreaming:       true,
	}
}

// ConfigureAuth configures authentication
func (d *StarRocksDriver) ConfigureAuth(authConfig interface{}) error {
	return nil
}

// ExecutePipelineQuery executes a query using StarRocks pipeline engine
func (d *StarRocksDriver) ExecutePipelineQuery(ctx context.Context, db *sql.DB, sql string) (*StarRocksPipelineResult, error) {
	// Set session variable to enable pipeline
	_, err := db.ExecContext(ctx, "SET enable_pipeline_engine = 'true'")
	if err != nil {
		return nil, fmt.Errorf("failed to enable pipeline engine: %w", err)
	}

	rows, err := db.QueryContext(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to execute pipeline query: %w", err)
	}
	defer rows.Close()

	// Read results
	columns, _ := rows.Columns()
	var results []map[string]interface{}

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		rowMap := make(map[string]interface{})
		for i, col := range columns {
			rowMap[col] = values[i]
		}
		results = append(results, rowMap)
	}

	return &StarRocksPipelineResult{
		Rows:         results,
		Columns:      columns,
		RowCount:     len(results),
		PipelineUsed: d.config.EnablePipeline,
	}, nil
}

// StarRocksPipelineResult represents StarRocks pipeline query results
type StarRocksPipelineResult struct {
	Rows         []map[string]interface{}
	Columns      []string
	RowCount     int
	PipelineUsed bool
}

// GetTableDistribution retrieves table distribution information
func (d *StarRocksDriver) GetTableDistribution(ctx context.Context, db *sql.DB, tableName string) (*StarRocksTableDistribution, error) {
	sql := `
		SELECT TABLE_NAME, BUCKET_NUM, REPLICATION_NUM
		FROM information_schema.TABLES
		WHERE TABLE_NAME = ?
	`

	dist := &StarRocksTableDistribution{
		TableName: tableName,
	}

	err := db.QueryRowContext(ctx, sql, tableName).Scan(
		&dist.TableName,
		&dist.BucketNum,
		&dist.ReplicationNum,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get table distribution: %w", err)
	}

	return dist, nil
}

// StarRocksTableDistribution represents StarRocks table distribution
type StarRocksTableDistribution struct {
	TableName      string
	BucketNum      int
	ReplicationNum int
}

// RegisterStarRocksDriver registers the StarRocks driver globally
func RegisterStarRocksDriver(config *StarRocksConfig) error {
	driver, err := NewStarRocksDriver(config)
	if err != nil {
		return err
	}

	database.GetDriverRegistry().RegisterDriver(model.DatabaseTypeStarRocks, driver)
	return nil
}
