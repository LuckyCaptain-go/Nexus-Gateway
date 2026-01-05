package domestic

import (
	"context"
	"database/sql"
	"fmt"
	"nexus-gateway/internal/database/drivers"
	"time"

	"nexus-gateway/internal/model"

	_ "github.com/lib/pq"
)

// KingbaseESDriver implements Driver interface for KingbaseES
type KingbaseESDriver struct {
	config *KingbaseESConfig
}

// KingbaseESConfig holds KingbaseES configuration
type KingbaseESConfig struct {
	Host           string
	Port           int
	Database       string
	Username       string
	Password       string
	Schema         string
	SSLMode        string // disable, require, verify-ca, verify-full
	CompatibleMode string // PG, ORA, MY (PostgreSQL, Oracle, MySQL compatibility)
}

// NewKingbaseESDriver creates a new KingbaseES driver
func NewKingbaseESDriver(config *KingbaseESConfig) (*KingbaseESDriver, error) {
	if config.Host == "" {
		return nil, fmt.Errorf("host is required")
	}

	return &KingbaseESDriver{
		config: config,
	}, nil
}

// Open opens a connection to KingbaseES (PostgreSQL-compatible)
func (d *KingbaseESDriver) Open(dsn string) (*sql.DB, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open KingbaseES connection: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping KingbaseES: %w", err)
	}

	return db, nil
}

// ValidateDSN validates the connection string
func (d *KingbaseESDriver) ValidateDSN(dsn string) error {
	if dsn == "" {
		return fmt.Errorf("DSN cannot be empty")
	}
	return nil
}

// GetDefaultPort returns the default KingbaseES port
func (d *KingbaseESDriver) GetDefaultPort() int {
	return 54321
}

// BuildDSN builds a connection string from configuration
func (d *KingbaseESDriver) BuildDSN(config *model.DataSourceConfig) string {
	sslMode := d.config.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}

	dsn := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=%s",
		config.Host, config.Port, config.Database, config.Username, config.Password, sslMode)

	// Add schema if specified
	if d.config.Schema != "" {
		dsn += " search_path=" + d.config.Schema
	}

	return dsn
}

// GetDatabaseTypeName returns the database type name
func (d *KingbaseESDriver) GetDatabaseTypeName() string {
	return "kingbasees"
}

// TestConnection tests if the connection is working
func (d *KingbaseESDriver) TestConnection(db *sql.DB) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	return db.PingContext(ctx)
}

// GetDriverName returns the driver name
func (d *KingbaseESDriver) GetDriverName() string {
	return "kingbasees-pg"
}

// GetCategory returns the driver category
func (d *KingbaseESDriver) GetCategory() drivers.DriverCategory {
	return drivers.CategoryRelational
}

// GetCapabilities returns driver capabilities
func (d *KingbaseESDriver) GetCapabilities() drivers.DriverCapabilities {
	return drivers.DriverCapabilities{
		SupportsSQL:             true,
		SupportsTransaction:     true,
		SupportsSchemaDiscovery: true,
		SupportsTimeTravel:      false,
		RequiresTokenRotation:   false,
		SupportsStreaming:       false,
	}
}

// ConfigureAuth configures authentication
func (d *KingbaseESDriver) ConfigureAuth(authConfig interface{}) error {
	return nil
}

// GetCompatibleMode returns the compatibility mode
func (d *KingbaseESDriver) GetCompatibleMode() string {
	return d.config.CompatibleMode
}

// SetCompatibleMode sets the compatibility mode
func (d *KingbaseESDriver) SetCompatibleMode(ctx context.Context, db *sql.DB, mode string) error {
	// KingbaseES supports compatibility modes
	_, err := db.ExecContext(ctx, fmt.Sprintf("SET compatible_mode = %s", mode))
	return err
}

// GetDatabaseInfo retrieves KingbaseES database information
func (d *KingbaseESDriver) GetDatabaseInfo(ctx context.Context, db *sql.DB) (*KingbaseESDatabaseInfo, error) {
	var version string
	err := db.QueryRowContext(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		return nil, fmt.Errorf("failed to get version: %w", err)
	}

	info := &KingbaseESDatabaseInfo{
		Version: version,
	}

	// Get compatible mode
	err = db.QueryRowContext(ctx, "SHOW compatible_mode").Scan(&info.CompatibleMode)
	if err != nil {
		info.CompatibleMode = "PG" // Default
	}

	return info, nil
}

// KingbaseESDatabaseInfo represents KingbaseES database information
type KingbaseESDatabaseInfo struct {
	Version        string
	CompatibleMode string
	Edition        string // Enterprise, Standard
}

// GetSequenceInfo retrieves sequence information
func (d *KingbaseESDriver) GetSequenceInfo(ctx context.Context, db *sql.DB, sequenceName string) (*KingbaseESSequenceInfo, error) {
	sql := `
		SELECT SEQUENCE_NAME, LAST_VALUE, START_VALUE,
		       INCREMENT_BY, MAX_VALUE, MIN_VALUE, CYCLE
		FROM USER_SEQUENCES
		WHERE SEQUENCE_NAME = ?
	`

	info := &KingbaseESSequenceInfo{
		SequenceName: sequenceName,
	}

	err := db.QueryRowContext(ctx, sql, sequenceName).Scan(
		&info.SequenceName,
		&info.LastValue,
		&info.StartValue,
		&info.IncrementBy,
		&info.MaxValue,
		&info.MinValue,
		&info.Cycle,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get sequence info: %w", err)
	}

	return info, nil
}

// KingbaseESSequenceInfo represents sequence information
type KingbaseESSequenceInfo struct {
	SequenceName string
	LastValue    int64
	StartValue   int64
	IncrementBy  int64
	MaxValue     int64
	MinValue     int64
	Cycle        bool
}

// ExecuteOracleCompatQuery executes a query in Oracle compatibility mode
func (d *KingbaseESDriver) ExecuteOracleCompatQuery(ctx context.Context, db *sql.DB, sql string) (*KingbaseESQueryResult, error) {
	// Set Oracle compatibility mode for this session
	_, err := db.ExecContext(ctx, "SET compatible_mode = 'ORA'")
	if err != nil {
		return nil, fmt.Errorf("failed to set Oracle mode: %w", err)
	}

	rows, err := db.QueryContext(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
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

	return &KingbaseESQueryResult{
		Rows:       results,
		Columns:    columns,
		Count:      len(results),
		CompatMode: "ORA",
	}, nil
}

// KingbaseESQueryResult represents query results
type KingbaseESQueryResult struct {
	Rows       []map[string]interface{}
	Columns    []string
	Count      int
	CompatMode string
}

// RegisterKingbaseESDriver registers the KingbaseES driver globally
func RegisterKingbaseESDriver(config *KingbaseESConfig) error {
	driver, err := NewKingbaseESDriver(config)
	if err != nil {
		return err
	}
	// Registration should be handled by the central DriverRegistry
	// (e.g. in internal/database/driver_registry.go) to avoid import cycles.
	_ = driver
	return nil
}
