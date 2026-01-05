package olap

import (
	"context"
	"database/sql"
	"fmt"
	"nexus-gateway/internal/database/drivers"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"nexus-gateway/internal/database"
	"nexus-gateway/internal/model"
)

// ClickHouseDriver implements Driver interface for ClickHouse
type ClickHouseDriver struct {
	connection *clickhouse.Conn
	config     *ClickHouseConfig
}

// ClickHouseConfig holds ClickHouse configuration
type ClickHouseConfig struct {
	Host        string
	Port        int
	Database    string
	Username    string
	Password    string
	DialTimeout time.Duration
}

// NewClickHouseDriver creates a new ClickHouse driver
func NewClickHouseDriver(config *ClickHouseConfig) (*ClickHouseDriver, error) {
	if config.Host == "" {
		return nil, fmt.Errorf("host is required")
	}

	// Build ClickHouse DSN
	dsn := fmt.Sprintf("clickhouse://%s:%s@%s:%d/%s",
		config.Username,
		config.Password,
		config.Host,
		config.Port,
		config.Database,
	)

	// Create ClickHouse connection options
	options := make(clickhouse.Options, 0)
	if config.DialTimeout > 0 {
		options = append(options, clickhouse.WithDialTimeout(func(ctx context.Context, addr string) (context.Context, error) {
			ctx, cancel := context.WithTimeout(ctx, config.DialTimeout)
			return ctx, cancel
		}))
	}

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", config.Host, config.Port)},
		Auth: clickhouse.Auth{
			Database: config.Database,
			Username: config.Username,
			Password: config.Password,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	return &ClickHouseDriver{
		connection: conn,
		config:     config,
	}, nil
}

// Open opens a connection to ClickHouse
func (d *ClickHouseDriver) Open(dsn string) (*sql.DB, error) {
	// Use ClickHouse native driver
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", d.config.Host, d.config.Port)},
		Auth: clickhouse.Auth{
			Database: d.config.Database,
			Username: d.config.Username,
			Password: d.config.Password,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open ClickHouse connection: %w", err)
	}

	// Wrap in sql.DB
	db := sql.OpenDB("clickhouse", conn)
	return db, nil
}

// ValidateDSN validates the connection string
func (d *ClickHouseDriver) ValidateDSN(dsn string) error {
	if dsn == "" {
		return fmt.Errorf("DSN cannot be empty")
	}
	return nil
}

// GetDefaultPort returns the default ClickHouse port
func (d *ClickHouseDriver) GetDefaultPort() int {
	return 9000 // Native protocol port
}

// BuildDSN builds a connection string from configuration
func (d *ClickHouseDriver) BuildDSN(config *model.DataSourceConfig) string {
	return fmt.Sprintf("clickhouse://%s:%s@%s:%d/%s",
		config.Username, config.Password, config.Host, config.Port, config.Database)
}

// GetDatabaseTypeName returns the database type name
func (d *ClickHouseDriver) GetDatabaseTypeName() string {
	return "clickhouse"
}

// TestConnection tests if the connection is working
func (d *ClickHouseDriver) TestConnection(db *sql.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return db.PingContext(ctx)
}

// GetDriverName returns the driver name
func (d *ClickHouseDriver) GetDriverName() string {
	return "clickhouse-native"
}

// GetCategory returns the driver category
func (d *ClickHouseDriver) GetCategory() drivers.DriverCategory {
	return drivers.CategoryOLAP
}

// GetCapabilities returns driver capabilities
func (d *ClickHouseDriver) GetCapabilities() drivers.DriverCapabilities {
	return drivers.DriverCapabilities{
		SupportsSQL:             true,
		SupportsTransaction:     false, // ClickHouse doesn't support traditional transactions
		SupportsSchemaDiscovery: true,
		SupportsTimeTravel:      false,
		RequiresTokenRotation:   false,
		SupportsStreaming:       true,
	}
}

// ConfigureAuth configures authentication
func (d *ClickHouseDriver) ConfigureAuth(authConfig interface{}) error {
	return nil
}

// ExecuteQuery executes a ClickHouse query
func (d *ClickHouseDriver) ExecuteQuery(ctx context.Context, sql string) (*ClickHouseQueryResult, error) {
	err := d.connection.Ping(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	// Execute query
	rows, err := d.connection.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Read results
	var results []map[string]interface{}
	for rows.Next() {
		row := make(map[string]interface{})
		if err := rows.ScanStruct(&row); err == nil {
			results = append(results, row)
		}
	}

	return &ClickHouseQueryResult{
		Rows:  results,
		Count: len(results),
	}, nil
}

// ClickHouseQueryResult represents ClickHouse query results
type ClickHouseQueryResult struct {
	Rows  []map[string]interface{}
	Count int
}

// GetTableSchema retrieves table schema from ClickHouse
func (d *ClickHouseDriver) GetTableSchema(ctx context.Context, tableName string) (*ClickHouseTableSchema, error) {
	sql := fmt.Sprintf("DESCRIBE TABLE %s", tableName)
	result, err := d.ExecuteQuery(ctx, sql)
	if err != nil {
		return nil, err
	}

	schema := &ClickHouseTableSchema{
		TableName: tableName,
		Columns:   make([]ClickHouseColumn, 0),
	}

	for _, row := range result.Rows {
		col := ClickHouseColumn{
			Name:     row["name"].(string),
			Type:     row["type"].(string),
			Nullable: row["default_kind"] != nil,
		}
		schema.Columns = append(schema.Columns, col)
	}

	return schema, nil
}

// ClickHouseTableSchema represents ClickHouse table schema
type ClickHouseTableSchema struct {
	TableName string
	Columns   []ClickHouseColumn
}

// ClickHouseColumn represents a ClickHouse column
type ClickHouseColumn struct {
	Name     string
	Type     string
	Nullable bool
	Default  interface{}
}

// RegisterClickHouseDriver registers the ClickHouse driver globally
func RegisterClickHouseDriver(config *ClickHouseConfig) error {
	driver, err := NewClickHouseDriver(config)
	if err != nil {
		return err
	}

	database.GetDriverRegistry().RegisterDriver(model.DatabaseTypeClickHouse, driver)
	return nil
}
