package domestic

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/sijms/go-ora/v2"
	"nexus-gateway/internal/database"
	"nexus-gateway/internal/model"
)

// OceanBaseOracleDriver implements Driver interface for OceanBase in Oracle mode
type OceanBaseOracleDriver struct {
	config *OceanBaseOracleConfig
}

// OceanBaseOracleConfig holds OceanBase Oracle mode configuration
type OceanBaseOracleConfig struct {
	Host         string
	Port         int
	Database     string // Usually 'SYS' or service name
	Username     string
	Password     string
	Schema       string
	ConnectDesc  string // Connection description string (TNS-like)
	Role         string // SYSDBA, SYSOPER, etc.
	Charset      string // AL32UTF8, ZHS16GBK, etc.
	Edition      string // Edition for edition-based redefinition
}

// NewOceanBaseOracleDriver creates a new OceanBase Oracle mode driver
func NewOceanBaseOracleDriver(config *OceanBaseOracleConfig) (*OceanBaseOracleDriver, error) {
	if config.Host == "" {
		return nil, fmt.Errorf("host is required")
	}

	return &OceanBaseOracleDriver{
		config: config,
	}, nil
}

// Open opens a connection to OceanBase in Oracle mode
func (d *OceanBaseOracleDriver) Open(dsn string) (*sql.DB, error) {
	db, err := sql.Open("oracle", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open OceanBase Oracle connection: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping OceanBase Oracle: %w", err)
	}

	return db, nil
}

// ValidateDSN validates the connection string
func (d *OceanBaseOracleDriver) ValidateDSN(dsn string) error {
	if dsn == "" {
		return fmt.Errorf("DSN cannot be empty")
	}
	return nil
}

// GetDefaultPort returns the default OceanBase Oracle port
func (d *OceanBaseOracleDriver) GetDefaultPort() int {
	return 2883
}

// BuildDSN builds a connection string from configuration
func (d *OceanBaseOracleDriver) BuildDSN(config *model.DataSourceConfig) string {
	charset := d.config.Charset
	if charset == "" {
		charset = "AL32UTF8"
	}

	// Build Oracle-style connection string
	dsn := fmt.Sprintf("oracle://%s:%s@%s:%d/%s",
		config.Username, config.Password, config.Host, config.Port, config.Database)

	// Add schema if specified
	if d.config.Schema != "" {
		dsn += "?schema=" + d.config.Schema
	}

	// Add charset
	dsn += "&charset=" + charset

	return dsn
}

// GetDatabaseTypeName returns the database type name
func (d *OceanBaseOracleDriver) GetDatabaseTypeName() string {
	return "oceanbase-oracle"
}

// TestConnection tests if the connection is working
func (d *OceanBaseOracleDriver) TestConnection(db *sql.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return db.PingContext(ctx)
}

// GetDriverName returns the driver name
func (d *OceanBaseOracleDriver) GetDriverName() string {
	return "oceanbase-oracle"
}

// GetCategory returns the driver category
func (d *OceanBaseOracleDriver) GetCategory() database.DriverCategory {
	return database.CategoryRelational
}

// GetCapabilities returns driver capabilities
func (d *OceanBaseOracleDriver) GetCapabilities() database.DriverCapabilities {
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
func (d *OceanBaseOracleDriver) ConfigureAuth(authConfig interface{}) error {
	return nil
}

// QueryWithHints executes a query with Oracle hints
func (d *OceanBaseOracleDriver) QueryWithHints(ctx context.Context, db *sql.DB, sql string, hints []string) (*OceanBaseOracleResult, error) {
	// Prepend hints to SQL
	if len(hints) > 0 {
		hintStr := "/*+ " + fmt.Sprintf(" %s", hints) + " */"
		sql = hintStr + " " + sql
	}

	rows, err := db.QueryContext(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to execute hinted query: %w", err)
	}
	defer rows.Close()

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

	return &OceanBaseOracleResult{
		Rows:    results,
		Columns: columns,
		Count:   len(results),
		Hints:   hints,
	}, nil
}

// OceanBaseOracleResult represents OceanBase Oracle query results
type OceanBaseOracleResult struct {
	Rows    []map[string]interface{}
	Columns []string
	Count   int
	Hints   []string
}

// GetPLSQLBlockResult executes a PL/SQL block
func (d *OceanBaseOracleDriver) GetPLSQLBlockResult(ctx context.Context, db *sql.DB, plsqlBlock string) (*OceanBasePLSQLResult, error) {
	_, err := db.ExecContext(ctx, plsqlBlock)
	if err != nil {
		return nil, fmt.Errorf("failed to execute PL/SQL block: %w", err)
	}

	return &OceanBasePLSQLResult{
		Executed: true,
		Message:  "PL/SQL block executed successfully",
	}, nil
}

// OceanBasePLSQLResult represents PL/SQL execution result
type OceanBasePLSQLResult struct {
	Executed bool
	Message  string
	BindVars map[string]interface{}
}

// GetOracleCompatibilityMode returns Oracle compatibility mode info
func (d *OceanBaseOracleDriver) GetOracleCompatibilityMode(ctx context.Context, db *sql.DB) (*OceanBaseCompatibilityInfo, error) {
	var version, compatibility string
	err := db.QueryRowContext(ctx, "SELECT VERSION(), @@compatibility_version").Scan(&version, &compatibility)
	if err != nil {
		return nil, fmt.Errorf("failed to get compatibility info: %w", err)
	}

	return &OceanBaseCompatibilityInfo{
		Version:      version,
		Compatibility: compatibility,
		Mode:         "ORACLE",
	}, nil
}

// OceanBaseCompatibilityInfo represents compatibility mode info
type OceanBaseCompatibilityInfo struct {
	Version      string
	Compatibility string
	Mode         string
}

// RegisterOceanBaseOracleDriver registers the OceanBase Oracle driver globally
func RegisterOceanBaseOracleDriver(config *OceanBaseOracleConfig) error {
	driver, err := NewOceanBaseOracleDriver(config)
	if err != nil {
		return err
	}

	database.GetDriverRegistry().RegisterDriver(model.DatabaseTypeOceanBaseOracle, driver)
	return nil
}
