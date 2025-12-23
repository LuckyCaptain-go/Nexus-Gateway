package database

import (
	"database/sql"
	"fmt"

	"nexus-gateway/internal/model"
)

// DriverFactory creates database drivers for different database types
type DriverFactory struct{}

// NewDriverFactory creates a new DriverFactory instance
func NewDriverFactory() *DriverFactory {
	return &DriverFactory{}
}

// CreateDriver creates a database driver for the specified database type
func (df *DriverFactory) CreateDriver(dbType model.DatabaseType) (Driver, error) {
	switch dbType {
	case model.DatabaseTypeMySQL, model.DatabaseTypeMariaDB:
		return &MySQLDriver{}, nil
	case model.DatabaseTypePostgreSQL:
		return &PostgreSQLDriver{}, nil
	case model.DatabaseTypeOracle:
		return &OracleDriver{}, nil
	default:
		return nil, fmt.Errorf("unsupported database type: %s", dbType)
	}
}

// Driver interface defines database-specific operations
type Driver interface {
	// Open opens a database connection
	Open(dsn string) (*sql.DB, error)

	// ValidateDSN validates the connection string
	ValidateDSN(dsn string) error

	// GetDefaultPort returns the default port for the database
	GetDefaultPort() int

	// BuildDSN builds a connection string from configuration
	BuildDSN(config *model.DataSourceConfig) string

	// GetDatabaseTypeName returns the database type name
	GetDatabaseTypeName() string

	// TestConnection tests if the connection is working
	TestConnection(db *sql.DB) error

	// GetDriverName returns the underlying SQL driver name
	GetDriverName() string
}

// MySQLDriver implements Driver for MySQL/MariaDB
type MySQLDriver struct{}

func (d *MySQLDriver) Open(dsn string) (*sql.DB, error) {
	return sql.Open("mysql", dsn)
}

func (d *MySQLDriver) ValidateDSN(dsn string) error {
	// Basic MySQL DSN validation
	if dsn == "" {
		return fmt.Errorf("DSN cannot be empty")
	}
	// More sophisticated validation could be added here
	return nil
}

func (d *MySQLDriver) GetDefaultPort() int {
	return 3306
}

func (d *MySQLDriver) BuildDSN(config *model.DataSourceConfig) string {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		config.Username,
		config.Password,
		config.Host,
		config.Port,
		config.Database,
	)

	// Add parameters
	params := []string{}
	if config.SSL {
		params = append(params, "tls=true")
	}
	if config.Timezone != "" {
		params = append(params, "loc="+config.Timezone)
	}

	if len(params) > 0 {
		dsn += "?"
		for i, param := range params {
			if i > 0 {
				dsn += "&"
			}
			dsn += param
		}
	}

	return dsn
}

func (d *MySQLDriver) GetDatabaseTypeName() string {
	return "mysql"
}

func (d *MySQLDriver) TestConnection(db *sql.DB) error {
	return db.Ping()
}

func (d *MySQLDriver) GetDriverName() string {
	return "mysql"
}

// PostgreSQLDriver implements Driver for PostgreSQL
type PostgreSQLDriver struct{}

func (d *PostgreSQLDriver) Open(dsn string) (*sql.DB, error) {
	return sql.Open("postgres", dsn)
}

func (d *PostgreSQLDriver) ValidateDSN(dsn string) error {
	if dsn == "" {
		return fmt.Errorf("DSN cannot be empty")
	}
	return nil
}

func (d *PostgreSQLDriver) GetDefaultPort() int {
	return 5432
}

func (d *PostgreSQLDriver) BuildDSN(config *model.DataSourceConfig) string {
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		config.Username,
		config.Password,
		config.Host,
		config.Port,
		config.Database,
	)

	// Add parameters
	params := []string{}
	if config.SSL {
		params = append(params, "sslmode=require")
	} else {
		params = append(params, "sslmode=disable")
	}
	if config.Timezone != "" {
		params = append(params, "TimeZone="+config.Timezone)
	}

	if len(params) > 0 {
		dsn += "?"
		for i, param := range params {
			if i > 0 {
				dsn += "&"
			}
			dsn += param
		}
	}

	return dsn
}

func (d *PostgreSQLDriver) GetDatabaseTypeName() string {
	return "postgresql"
}

func (d *PostgreSQLDriver) TestConnection(db *sql.DB) error {
	return db.Ping()
}

func (d *PostgreSQLDriver) GetDriverName() string {
	return "postgres"
}

// OracleDriver implements Driver for Oracle
type OracleDriver struct{}

func (d *OracleDriver) Open(dsn string) (*sql.DB, error) {
	return sql.Open("oracle", dsn)
}

func (d *OracleDriver) ValidateDSN(dsn string) error {
	if dsn == "" {
		return fmt.Errorf("DSN cannot be empty")
	}
	return nil
}

func (d *OracleDriver) GetDefaultPort() int {
	return 1521
}

func (d *OracleDriver) BuildDSN(config *model.DataSourceConfig) string {
	// Oracle DSN format: user/password@host:port/database
	return fmt.Sprintf("%s/%s@%s:%d/%s",
		config.Username,
		config.Password,
		config.Host,
		config.Port,
		config.Database,
	)
}

func (d *OracleDriver) GetDatabaseTypeName() string {
	return "oracle"
}

func (d *OracleDriver) TestConnection(db *sql.DB) error {
	return db.Ping()
}

func (d *OracleDriver) GetDriverName() string {
	return "oracle"
}

// DriverRegistry holds registered drivers
type DriverRegistry struct {
	drivers map[model.DatabaseType]Driver
	factory *DriverFactory
}

// NewDriverRegistry creates a new DriverRegistry
func NewDriverRegistry() *DriverRegistry {
	registry := &DriverRegistry{
		drivers: make(map[model.DatabaseType]Driver),
		factory: NewDriverFactory(),
	}

	// Register built-in drivers
	registry.RegisterBuiltInDrivers()

	return registry
}

// RegisterBuiltInDrivers registers all built-in database drivers
func (dr *DriverRegistry) RegisterBuiltInDrivers() {
	mysqlDriver := &MySQLDriver{}
	postgresqlDriver := &PostgreSQLDriver{}
	oracleDriver := &OracleDriver{}

	dr.drivers[model.DatabaseTypeMySQL] = mysqlDriver
	dr.drivers[model.DatabaseTypeMariaDB] = mysqlDriver // MariaDB uses MySQL driver
	dr.drivers[model.DatabaseTypePostgreSQL] = postgresqlDriver
	dr.drivers[model.DatabaseTypeOracle] = oracleDriver
}

// GetDriver returns a driver for the specified database type
func (dr *DriverRegistry) GetDriver(dbType model.DatabaseType) (Driver, error) {
	if driver, exists := dr.drivers[dbType]; exists {
		return driver, nil
	}
	return nil, fmt.Errorf("no driver registered for database type: %s", dbType)
}

// RegisterDriver registers a custom driver
func (dr *DriverRegistry) RegisterDriver(dbType model.DatabaseType, driver Driver) {
	dr.drivers[dbType] = driver
}

// GetSupportedTypes returns all supported database types
func (dr *DriverRegistry) GetSupportedTypes() []model.DatabaseType {
	types := make([]model.DatabaseType, 0, len(dr.drivers))
	for dbType := range dr.drivers {
		types = append(types, dbType)
	}
	return types
}

// Global driver registry instance
var globalDriverRegistry = NewDriverRegistry()

// GetDriverRegistry returns the global driver registry
func GetDriverRegistry() *DriverRegistry {
	return globalDriverRegistry
}