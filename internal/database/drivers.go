package database

import (
	"database/sql"
	"fmt"

	"nexus-gateway/internal/database/drivers/traditional"
	"nexus-gateway/internal/model"
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
		return &traditional.MySQLDriver{}, nil
	case model.DatabaseTypePostgreSQL:
		return &traditional.PostgreSQLDriver{}, nil
	case model.DatabaseTypeOracle:
		return &traditional.OracleDriver{}, nil
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

	// GetCategory returns the driver category
	GetCategory() DriverCategory

	// GetCapabilities returns driver capabilities
	GetCapabilities() DriverCapabilities

	// ConfigureAuth configures authentication (for IAM/OAuth2/Kerberos)
	ConfigureAuth(authConfig interface{}) error
}

// =============================================================================
// Placeholder Drivers for Phase 1 Data Sources
// These drivers are registered but not yet fully implemented
// =============================================================================

// PlaceholderDriver represents a driver that is planned but not yet implemented
type PlaceholderDriver struct {
	dbType   model.DatabaseType
	category DriverCategory
}

func NewPlaceholderDriver(dbType model.DatabaseType, category DriverCategory) *PlaceholderDriver {
	return &PlaceholderDriver{dbType: dbType, category: category}
}

func (d *PlaceholderDriver) Open(dsn string) (*sql.DB, error) {
	return nil, fmt.Errorf("driver for %s is not yet implemented", d.dbType)
}

func (d *PlaceholderDriver) ValidateDSN(dsn string) error {
	return nil
}

func (d *PlaceholderDriver) GetDefaultPort() int {
	return 0 // Unknown for placeholder
}

func (d *PlaceholderDriver) BuildDSN(config *model.DataSourceConfig) string {
	return ""
}

func (d *PlaceholderDriver) GetDatabaseTypeName() string {
	return string(d.dbType)
}

func (d *PlaceholderDriver) TestConnection(db *sql.DB) error {
	return fmt.Errorf("driver for %s is not yet implemented", d.dbType)
}

func (d *PlaceholderDriver) GetDriverName() string {
	return string(d.dbType)
}

func (d *PlaceholderDriver) GetCategory() DriverCategory {
	return d.category
}

func (d *PlaceholderDriver) GetCapabilities() DriverCapabilities {
	return DriverCapabilities{
		SupportsSQL:             d.category == CategoryTableFormat || d.category == CategoryWarehouse || d.category == CategoryOLAP,
		SupportsTransaction:     false,
		SupportsSchemaDiscovery: true,
		SupportsTimeTravel:      d.category == CategoryTableFormat,
		RequiresTokenRotation:   d.category == CategoryWarehouse || d.category == CategoryObjectStorage || d.category == CategoryFileSystem,
		SupportsStreaming:       true,
	}
}

func (d *PlaceholderDriver) ConfigureAuth(authConfig interface{}) error {
	return fmt.Errorf("driver for %s is not yet implemented", d.dbType)
}

// =============================================================================
// Table Format Drivers (Iceberg, Delta Lake, Hudi)
// =============================================================================

// IcebergDriver for Apache Iceberg tables
type IcebergDriver struct{ *PlaceholderDriver }

func NewIcebergDriver() *IcebergDriver {
	return &IcebergDriver{PlaceholderDriver: NewPlaceholderDriver(model.DatabaseTypeApacheIceberg, CategoryTableFormat)}
}

// DeltaLakeDriver for Delta Lake tables
type DeltaLakeDriver struct{ *PlaceholderDriver }

func NewDeltaLakeDriver() *DeltaLakeDriver {
	return &DeltaLakeDriver{PlaceholderDriver: NewPlaceholderDriver(model.DatabaseTypeDeltaLake, CategoryTableFormat)}
}

// HudiDriver for Apache Hudi tables
type HudiDriver struct{ *PlaceholderDriver }

func NewHudiDriver() *HudiDriver {
	return &HudiDriver{PlaceholderDriver: NewPlaceholderDriver(model.DatabaseTypeApacheHudi, CategoryTableFormat)}
}

// =============================================================================
// Cloud Warehouse Drivers
// =============================================================================

// SnowflakeDriver for Snowflake
type SnowflakeDriver struct{ *PlaceholderDriver }

func NewSnowflakeDriver() *SnowflakeDriver {
	return &SnowflakeDriver{PlaceholderDriver: NewPlaceholderDriver(model.DatabaseTypeSnowflake, CategoryWarehouse)}
}

// DatabricksDriver for Databricks
type DatabricksDriver struct{ *PlaceholderDriver }

func NewDatabricksDriver() *DatabricksDriver {
	return &DatabricksDriver{PlaceholderDriver: NewPlaceholderDriver(model.DatabaseTypeDatabricks, CategoryWarehouse)}
}

// RedshiftDriver for Amazon Redshift
type RedshiftDriver struct{ *PlaceholderDriver }

func NewRedshiftDriver() *RedshiftDriver {
	return &RedshiftDriver{PlaceholderDriver: NewPlaceholderDriver(model.DatabaseTypeRedshift, CategoryWarehouse)}
}

// BigQueryDriver for Google BigQuery
type BigQueryDriver struct{ *PlaceholderDriver }

func NewBigQueryDriver() *BigQueryDriver {
	return &BigQueryDriver{PlaceholderDriver: NewPlaceholderDriver(model.DatabaseTypeBigQuery, CategoryWarehouse)}
}

// =============================================================================
// Object Storage Drivers
// =============================================================================

// S3Driver for AWS S3
type S3Driver struct{ *PlaceholderDriver }

func NewS3Driver() *S3Driver {
	return &S3Driver{PlaceholderDriver: NewPlaceholderDriver(model.DatabaseTypeS3, CategoryObjectStorage)}
}

// MinIODriver for MinIO
type MinIODriver struct{ *PlaceholderDriver }

func NewMinIODriver() *MinIODriver {
	return &MinIODriver{PlaceholderDriver: NewPlaceholderDriver(model.DatabaseTypeMinIO, CategoryObjectStorage)}
}

// AlibabaOSSDriver for Alibaba Cloud OSS
type AlibabaOSSDriver struct{ *PlaceholderDriver }

func NewAlibabaOSSDriver() *AlibabaOSSDriver {
	return &AlibabaOSSDriver{PlaceholderDriver: NewPlaceholderDriver(model.DatabaseTypeAlibabaOSS, CategoryObjectStorage)}
}

// TencentCOSDriver for Tencent Cloud COS
type TencentCOSDriver struct{ *PlaceholderDriver }

func NewTencentCOSDriver() *TencentCOSDriver {
	return &TencentCOSDriver{PlaceholderDriver: NewPlaceholderDriver(model.DatabaseTypeTencentCOS, CategoryObjectStorage)}
}

// AzureBlobDriver for Azure Blob Storage
type AzureBlobDriver struct{ *PlaceholderDriver }

func NewAzureBlobDriver() *AzureBlobDriver {
	return &AzureBlobDriver{PlaceholderDriver: NewPlaceholderDriver(model.DatabaseTypeAzureBlob, CategoryObjectStorage)}
}

// =============================================================================
// OLAP Engine Drivers
// =============================================================================

// ClickHouseDriver for ClickHouse
type ClickHouseDriver struct{ *PlaceholderDriver }

func NewClickHouseDriver() *ClickHouseDriver {
	return &ClickHouseDriver{PlaceholderDriver: NewPlaceholderDriver(model.DatabaseTypeClickHouse, CategoryOLAP)}
}

// ApacheDorisDriver for Apache Doris
type ApacheDorisDriver struct{ *PlaceholderDriver }

func NewApacheDorisDriver() *ApacheDorisDriver {
	return &ApacheDorisDriver{PlaceholderDriver: NewPlaceholderDriver(model.DatabaseTypeApacheDoris, CategoryOLAP)}
}

// StarRocksDriver for StarRocks
type StarRocksDriver struct{ *PlaceholderDriver }

func NewStarRocksDriver() *StarRocksDriver {
	return &StarRocksDriver{PlaceholderDriver: NewPlaceholderDriver(model.DatabaseTypeStarRocks, CategoryOLAP)}
}

// ApacheDruidDriver for Apache Druid
type ApacheDruidDriver struct{ *PlaceholderDriver }

func NewApacheDruidDriver() *ApacheDruidDriver {
	return &ApacheDruidDriver{PlaceholderDriver: NewPlaceholderDriver(model.DatabaseTypeApacheDruid, CategoryOLAP)}
}

// =============================================================================
// Domestic Database Drivers
// =============================================================================

// OceanBaseDriver for OceanBase
type OceanBaseDriver struct{ *PlaceholderDriver }

func NewOceanBaseDriver() *OceanBaseDriver {
	return &OceanBaseDriver{PlaceholderDriver: NewPlaceholderDriver(model.DatabaseTypeOceanBase, CategoryDomesticDatabase)}
}

// TiDBDriver for TiDB
type TiDBDriver struct{ *PlaceholderDriver }

func NewTiDBDriver() *TiDBDriver {
	return &TiDBDriver{PlaceholderDriver: NewPlaceholderDriver(model.DatabaseTypeTiDB, CategoryDomesticDatabase)}
}

// TDSQLDriver for Tencent TDSQL
type TDSQLDriver struct{ *PlaceholderDriver }

func NewTDSQLDriver() *TDSQLDriver {
	return &TDSQLDriver{PlaceholderDriver: NewPlaceholderDriver(model.DatabaseTypeTDSQL, CategoryDomesticDatabase)}
}

// GaussDBDriver for Huawei GaussDB
type GaussDBDriver struct{ *PlaceholderDriver }

func NewGaussDBDriver() *GaussDBDriver {
	return &GaussDBDriver{PlaceholderDriver: NewPlaceholderDriver(model.DatabaseTypeGaussDB, CategoryDomesticDatabase)}
}

// DaMengDriver for DaMeng
type DaMengDriver struct{ *PlaceholderDriver }

func NewDaMengDriver() *DaMengDriver {
	return &DaMengDriver{PlaceholderDriver: NewPlaceholderDriver(model.DatabaseTypeDaMeng, CategoryDomesticDatabase)}
}

// KingbaseESDriver for KingbaseES
type KingbaseESDriver struct{ *PlaceholderDriver }

func NewKingbaseESDriver() *KingbaseESDriver {
	return &KingbaseESDriver{PlaceholderDriver: NewPlaceholderDriver(model.DatabaseTypeKingbaseES, CategoryDomesticDatabase)}
}

// GBaseDriver for GBase
type GBaseDriver struct{ *PlaceholderDriver }

func NewGBaseDriver() *GBaseDriver {
	return &GBaseDriver{PlaceholderDriver: NewPlaceholderDriver(model.DatabaseTypeGBase, CategoryDomesticDatabase)}
}

// OscarDriver for Oscar
type OscarDriver struct{ *PlaceholderDriver }

func NewOscarDriver() *OscarDriver {
	return &OscarDriver{PlaceholderDriver: NewPlaceholderDriver(model.DatabaseTypeOscar, CategoryDomesticDatabase)}
}

// OpenGaussDriver for OpenGauss
type OpenGaussDriver struct{ *PlaceholderDriver }

func NewOpenGaussDriver() *OpenGaussDriver {
	return &OpenGaussDriver{PlaceholderDriver: NewPlaceholderDriver(model.DatabaseTypeOpenGauss, CategoryDomesticDatabase)}
}

// =============================================================================
// Distributed File System Drivers
// =============================================================================

// HDFSDriver for HDFS
type HDFSDriver struct{ *PlaceholderDriver }

func NewHDFSDriver() *HDFSDriver {
	return &HDFSDriver{PlaceholderDriver: NewPlaceholderDriver(model.DatabaseTypeHDFS, CategoryFileSystem)}
}

// OzoneDriver for Apache Ozone
type OzoneDriver struct{ *PlaceholderDriver }

func NewOzoneDriver() *OzoneDriver {
	return &OzoneDriver{PlaceholderDriver: NewPlaceholderDriver(model.DatabaseTypeOzone, CategoryFileSystem)}
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
	// Relational databases (implemented)
	mysqlDriver := &MySQLDriver{}
	postgresqlDriver := &PostgreSQLDriver{}
	oracleDriver := &OracleDriver{}

	dr.drivers[model.DatabaseTypeMySQL] = mysqlDriver
	dr.drivers[model.DatabaseTypeMariaDB] = mysqlDriver // MariaDB uses MySQL driver
	dr.drivers[model.DatabaseTypePostgreSQL] = postgresqlDriver
	dr.drivers[model.DatabaseTypeOracle] = oracleDriver

	// Table Format Drivers (placeholders)
	dr.drivers[model.DatabaseTypeApacheIceberg] = NewIcebergDriver()
	dr.drivers[model.DatabaseTypeDeltaLake] = NewDeltaLakeDriver()
	dr.drivers[model.DatabaseTypeApacheHudi] = NewHudiDriver()

	// Cloud Warehouse Drivers (placeholders)
	dr.drivers[model.DatabaseTypeSnowflake] = NewSnowflakeDriver()
	dr.drivers[model.DatabaseTypeDatabricks] = NewDatabricksDriver()
	dr.drivers[model.DatabaseTypeRedshift] = NewRedshiftDriver()
	dr.drivers[model.DatabaseTypeBigQuery] = NewBigQueryDriver()

	// Object Storage Drivers (placeholders)
	dr.drivers[model.DatabaseTypeS3] = NewS3Driver()
	dr.drivers[model.DatabaseTypeMinIO] = NewMinIODriver()
	dr.drivers[model.DatabaseTypeAlibabaOSS] = NewAlibabaOSSDriver()
	dr.drivers[model.DatabaseTypeTencentCOS] = NewTencentCOSDriver()
	dr.drivers[model.DatabaseTypeAzureBlob] = NewAzureBlobDriver()

	// OLAP Engine Drivers (placeholders)
	dr.drivers[model.DatabaseTypeClickHouse] = NewClickHouseDriver()
	dr.drivers[model.DatabaseTypeApacheDoris] = NewApacheDorisDriver()
	dr.drivers[model.DatabaseTypeStarRocks] = NewStarRocksDriver()
	dr.drivers[model.DatabaseTypeApacheDruid] = NewApacheDruidDriver()

	// Domestic Database Drivers (placeholders)
	dr.drivers[model.DatabaseTypeOceanBase] = NewOceanBaseDriver()
	dr.drivers[model.DatabaseTypeTiDB] = NewTiDBDriver()
	dr.drivers[model.DatabaseTypeTDSQL] = NewTDSQLDriver()
	dr.drivers[model.DatabaseTypeGaussDB] = NewGaussDBDriver()
	dr.drivers[model.DatabaseTypeDaMeng] = NewDaMengDriver()
	dr.drivers[model.DatabaseTypeKingbaseES] = NewKingbaseESDriver()
	dr.drivers[model.DatabaseTypeGBase] = NewGBaseDriver()
	dr.drivers[model.DatabaseTypeOscar] = NewOscarDriver()
	dr.drivers[model.DatabaseTypeOpenGauss] = NewOpenGaussDriver()

	// Distributed File System Drivers (placeholders)
	dr.drivers[model.DatabaseTypeHDFS] = NewHDFSDriver()
	dr.drivers[model.DatabaseTypeOzone] = NewOzoneDriver()
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
