package domestic

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
	"nexus-gateway/internal/database"
	"nexus-gateway/internal/model"
)

// DaMengDriver implements Driver interface for DaMeng database
type DaMengDriver struct {
	config *DaMengConfig
}

// DaMengConfig holds DaMeng configuration
type DaMengConfig struct {
	Host     string
	Port     int
	Database string
	Username string
	Password string
	Charset  string // UTF8, GB18030
}

// NewDaMengDriver creates a new DaMeng driver
func NewDaMengDriver(config *DaMengConfig) (*DaMengDriver, error) {
	if config.Host == "" {
		return nil, fmt.Errorf("host is required")
	}

	return &DaMengDriver{
		config: config,
	}, nil
}

// Open opens a connection to DaMeng
func (d *DaMengDriver) Open(dsn string) (*sql.DB, error) {
	// DaMeng uses PostgreSQL-compatible protocol
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open DaMeng connection: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping DaMeng: %w", err)
	}

	return db, nil
}

// ValidateDSN validates the connection string
func (d *DaMengDriver) ValidateDSN(dsn string) error {
	if dsn == "" {
		return fmt.Errorf("DSN cannot be empty")
	}
	return nil
}

// GetDefaultPort returns the default DaMeng port
func (d *DaMengDriver) GetDefaultPort() int {
	return 5236
}

// BuildDSN builds a connection string from configuration
func (d *DaMengDriver) BuildDSN(config *model.DataSourceConfig) string {
	charset := d.config.Charset
	if charset == "" {
		charset = "UTF8"
	}
	return fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=disable",
		config.Host, config.Port, config.Database, config.Username, config.Password)
}

// GetDatabaseTypeName returns the database type name
func (d *DaMengDriver) GetDatabaseTypeName() string {
	return "dameng"
}

// TestConnection tests if the connection is working
func (d *DaMengDriver) TestConnection(db *sql.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return db.PingContext(ctx)
}

// GetDriverName returns the driver name
func (d *DaMengDriver) GetDriverName() string {
	return "dameng-postgres"
}

// GetCategory returns the driver category
func (d *DaMengDriver) GetCategory() database.DriverCategory {
	return database.CategoryRelational
}

// GetCapabilities returns driver capabilities
func (d *DaMengDriver) GetCapabilities() database.DriverCapabilities {
	return database.DriverCapabilities{
		SupportsSQL:             true,
		SupportsTransaction:     true,
		SupportsSchemaDiscovery: true,
		SupportsTimeTravel:      false,
		RequiresTokenRotation:   false,
		SupportsStreaming:       false,
	}
}

// ConfigureAuth configures authentication
func (d *DaMengDriver) ConfigureAuth(authConfig interface{}) error {
	return nil
}

// QueryWithCharset executes a query with specific charset
func (d *DaMengDriver) QueryWithCharset(ctx context.Context, db *sql.DB, sql string, charset string) (*DaMengQueryResult, error) {
	if charset != "" {
		// Set client charset
		_, err := db.ExecContext(ctx, fmt.Sprintf("SET CLIENT_ENCODING TO %s", charset))
		if err != nil {
			return nil, fmt.Errorf("failed to set charset: %w", err)
		}
	}

	rows, err := db.QueryContext(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
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

	return &DaMengQueryResult{
		Rows:    results,
		Columns: columns,
		Count:   len(results),
		Charset: charset,
	}, nil
}

// DaMengQueryResult represents DaMeng query results
type DaMengQueryResult struct {
	Rows    []map[string]interface{}
	Columns []string
	Count   int
	Charset string
}

// GetDatabaseInfo retrieves DaMeng database information
func (d *DaMengDriver) GetDatabaseInfo(ctx context.Context, db *sql.DB) (*DaMengDatabaseInfo, error) {
	var version string
	err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version)
	if err != nil {
		return nil, fmt.Errorf("failed to get version: %w", err)
	}

	info := &DaMengDatabaseInfo{
		Version: version,
		Charset: d.config.Charset,
	}

	return info, nil
}

// DaMengDatabaseInfo represents DaMeng database information
type DaMengDatabaseInfo struct {
	Version string
	Charset string
	Mode    string // Enterprise or Standard
}

// RegisterDaMengDriver registers the DaMeng driver globally
func RegisterDaMengDriver(config *DaMengConfig) error {
	driver, err := NewDaMengDriver(config)
	if err != nil {
		return err
	}

	database.GetDriverRegistry().RegisterDriver(model.DatabaseTypeDaMeng, driver)
	return nil
}
