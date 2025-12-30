package domestic

import (
	"database/sql"
)

// DriverCategory categorizes drivers by their type
type DriverCategory string

const (
	CategoryRelational       DriverCategory = "relational"
	CategoryTableFormat      DriverCategory = "table_format"
	CategoryWarehouse        DriverCategory = "warehouse"
	CategoryObjectStorage    DriverCategory = "object_storage"
	CategoryOLAP             DriverCategory = "olap"
	CategoryDomesticDatabase DriverCategory = "domestic_database"
	CategoryFileSystem       DriverCategory = "file_system"
)

// DriverCapabilities defines what operations a driver supports
type DriverCapabilities struct {
	SupportsSQL             bool
	SupportsTransaction     bool
	SupportsSchemaDiscovery bool
	SupportsTimeTravel      bool
	RequiresTokenRotation   bool
	SupportsStreaming       bool
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
	BuildDSN(config interface{}) string

	// GetDatabaseTypeName returns the database type name
	GetDatabaseTypeName() string

	// TestConnection tests if the connection is working
	TestConnection(db *sql.DB) error

	// GetDriverName returns the underlying SQL driver name
	GetDriverName() string

	// GetCategory returns the driver category
	GetCategory() DriverCategory

	// GetCapabilities returns the driver capabilities
	GetCapabilities() DriverCapabilities

	// ConfigureAuth configures authentication
	ConfigureAuth(authConfig interface{}) error
}
