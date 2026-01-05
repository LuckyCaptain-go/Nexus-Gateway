package database

import (
	"fmt"
	"nexus-gateway/internal/database/drivers"
	"nexus-gateway/internal/database/drivers/domestic"
	"nexus-gateway/internal/database/drivers/traditional"
	"sync"

	"nexus-gateway/internal/model"
)

// DriverRegistry manages driver instances and creation
type DriverRegistry struct {
	drivers map[model.DatabaseType]func() drivers.Driver
	mutex   sync.RWMutex
}

// NewDriverRegistry creates a new driver registry
func NewDriverRegistry() *DriverRegistry {
	registry := &DriverRegistry{
		drivers: make(map[model.DatabaseType]func() drivers.Driver),
	}

	// Register all drivers
	registry.registerDrivers()

	return registry
}

// registerDrivers registers all available drivers using optimized generic drivers
func (dr *DriverRegistry) registerDrivers() {
	dr.mutex.Lock()
	defer dr.mutex.Unlock()

	// Relational databases
	dr.register(model.DatabaseTypeMySQL, func() drivers.Driver {
		return &traditional.MySQLDriver{}
	})
	dr.register(model.DatabaseTypeMariaDB, func() drivers.Driver {
		return &traditional.MySQLDriver{}
	})
	dr.register(model.DatabaseTypePostgreSQL, func() drivers.Driver {
		return &traditional.PostgreSQLDriver{}
	})
	dr.register(model.DatabaseTypeOracle, func() drivers.Driver {
		return &traditional.OracleDriver{}
	})

	dr.register(model.DatabaseTypeDaMeng, func() drivers.Driver {
		return &domestic.DaMengDBAdapter{
			Inner: &domestic.DaMengDriver{},
		}
	})
	dr.register(model.DatabaseTypeKingbaseES, func() drivers.Driver {
		return &domestic.DaMengDBAdapter{
			Inner: &domestic.DaMengDriver{},
		}
	})

	//// Domestic databases - using generic domestic driver for now
	//domesticDBs := []model.DatabaseType{
	//	model.DatabaseTypeDaMeng,
	//	model.DatabaseTypeKingbaseES,
	//	// Note: Using placeholder names for types not defined in model
	//	"gbase_8s", "gbase_8t", "oscar", "opengauss",
	//}
	//for _, dbType := range domesticDBs {
	//	dr.register(dbType, func() Driver {
	//		return NewDomesticDriver(dbType)
	//	})
	//}
	//
	//// Table formats
	//tableFormatDBs := []model.DatabaseType{
	//	model.DatabaseTypeApacheIceberg,
	//	model.DatabaseTypeDeltaLake,
	//	model.DatabaseTypeApacheHudi,
	//}
	//for _, dbType := range tableFormatDBs {
	//	dr.register(dbType, func() Driver {
	//		return NewTableFormatDriver(dbType)
	//	})
	//}
	//
	//// Cloud warehouses
	//warehouseDBs := []model.DatabaseType{
	//	model.DatabaseTypeSnowflake,
	//	model.DatabaseTypeDatabricks,
	//	model.DatabaseTypeRedshift,
	//	model.DatabaseTypeBigQuery,
	//}
	//for _, dbType := range warehouseDBs {
	//	dr.register(dbType, func() Driver {
	//		return NewWarehouseDriver(dbType)
	//	})
	//}
	//
	//// Object storage - mapping specific types to generic object storage
	//objectStorageTypes := []model.DatabaseType{
	//	model.DatabaseTypeS3Parquet,
	//	model.DatabaseTypeS3ORC,
	//	model.DatabaseTypeS3Avro,
	//	model.DatabaseTypeS3CSV,
	//	model.DatabaseTypeS3JSON,
	//	model.DatabaseTypeMinIOParquet,
	//	model.DatabaseTypeMinIOCSV,
	//	model.DatabaseTypeAlibabaOSSParquet,
	//	model.DatabaseTypeTencentCOSParquet,
	//	model.DatabaseTypeAzureBlobParquet,
	//}
	//for _, dbType := range objectStorageTypes {
	//	dr.register(dbType, func() Driver {
	//		return NewObjectStorageDriver(dbType)
	//	})
	//}
	//
	//// OLAP
	//olapDBs := []model.DatabaseType{
	//	model.DatabaseTypeClickHouse,
	//	model.DatabaseTypeApacheDoris,
	//	model.DatabaseTypeStarRocks,
	//	model.DatabaseTypeApacheDruid,
	//}
	//for _, dbType := range olapDBs {
	//	dr.register(dbType, func() Driver {
	//		return NewOLAPDriver(dbType)
	//	})
	//}
	//
	//// File systems
	//fileSystemDBs := []model.DatabaseType{
	//	model.DatabaseTypeHDFSAvro,
	//	model.DatabaseTypeHDFSParquet,
	//	model.DatabaseTypeHDFSCSV,
	//	model.DatabaseTypeOzoneParquet,
	//}
	//for _, dbType := range fileSystemDBs {
	//	dr.register(dbType, func() Driver {
	//		return NewFileSystemDriver(dbType)
	//	})
	//}
}

// register registers a driver factory function
func (dr *DriverRegistry) register(dbType model.DatabaseType, factory func() drivers.Driver) {
	dr.drivers[dbType] = factory
}

// GetDriver creates or retrieves a driver for the specified database type
func (dr *DriverRegistry) GetDriver(dbType model.DatabaseType) (drivers.Driver, error) {
	dr.mutex.RLock()
	factory, exists := dr.drivers[dbType]
	dr.mutex.RUnlock()

	if !exists {
		return nil, fmt.Errorf("unsupported database type: %s", dbType)
	}

	return factory(), nil
}

// ListDrivers returns all supported database types
func (dr *DriverRegistry) ListDrivers() []model.DatabaseType {
	dr.mutex.RLock()
	defer dr.mutex.RUnlock()

	types := make([]model.DatabaseType, 0, len(dr.drivers))
	for dbType := range dr.drivers {
		types = append(types, dbType)
	}

	return types
}

// IsSupported checks if a database type is supported
func (dr *DriverRegistry) IsSupported(dbType model.DatabaseType) bool {
	dr.mutex.RLock()
	_, exists := dr.drivers[dbType]
	dr.mutex.RUnlock()

	return exists
}

// GetDriverCategory returns the category for a database type
func (dr *DriverRegistry) GetDriverCategory(dbType model.DatabaseType) (drivers.DriverCategory, error) {
	driver, err := dr.GetDriver(dbType)
	if err != nil {
		return "", err
	}

	return driver.GetCategory(), nil
}

// GetSupportedTypes returns all supported database types
func (dr *DriverRegistry) GetSupportedTypes() []model.DatabaseType {
	return dr.ListDrivers()
}
