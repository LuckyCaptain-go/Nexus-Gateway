package database

import (
	"fmt"
	"nexus-gateway/internal/database/drivers"
	//"nexus-gateway/internal/database/drivers/domestic"
	//"nexus-gateway/internal/database/drivers/olap"
	//"nexus-gateway/internal/database/drivers/table_formats"
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

	// Chinese domestic databases
	//dr.register(model.DatabaseTypeDaMeng, func() drivers.Driver {
	//	return &domestic.DaMengDBAdapter{
	//		Inner: &domestic.DaMengDriver{},
	//	}
	//})
	//dr.register(model.DatabaseTypeOceanBaseMySQL, func() drivers.Driver {
	//	return &domestic.OceanBaseDriver{}
	//})
	//dr.register(model.DatabaseTypeOceanBaseOracle, func() drivers.Driver {
	//	return &domestic.OceanBaseOracleDriver{}
	//})
	//dr.register(model.DatabaseTypeTiDB, func() drivers.Driver {
	//	return &domestic.TiDBDriver{}
	//})
	//dr.register(model.DatabaseTypeTDSQL, func() drivers.Driver {
	//	return &domestic.TDSQLDriver{}
	//})
	//dr.register(model.DatabaseTypeGaussDBMySQL, func() drivers.Driver {
	//	return &domestic.GaussDBDriver{}
	//})
	//dr.register(model.DatabaseTypeGaussDBPostgres, func() drivers.Driver {
	//	return &domestic.GaussDBDriver{}
	//})
	//dr.register(model.DatabaseTypeKingbaseES, func() drivers.Driver {
	//	return &domestic.KingbaseESDriver{}
	//})
	//dr.register(model.DatabaseTypeGBase8s, func() drivers.Driver {
	//	return &domestic.GBaseDriver{}
	//})
	//dr.register(model.DatabaseTypeGBase8t, func() drivers.Driver {
	//	return &domestic.GBaseDriver{}
	//})
	//dr.register(model.DatabaseTypeOscar, func() drivers.Driver {
	//	return &domestic.OscarDriver{}
	//})
	//dr.register(model.DatabaseTypeOpenGauss, func() drivers.Driver {
	//	return &domestic.OpenGaussDriver{}
	//})
	//
	//// Data Lake Table Formats
	//dr.register(model.DatabaseTypeApacheIceberg, func() drivers.Driver {
	//	return &table_formats.IcebergDriver{}
	//})
	//dr.register(model.DatabaseTypeDeltaLake, func() drivers.Driver {
	//	return &table_formats.DeltaDriver{}
	//})
	//dr.register(model.DatabaseTypeApacheHudi, func() drivers.Driver {
	//	return &table_formats.HudiDriver{}
	//})

	//// Cloud Data Warehouses
	//dr.register(model.DatabaseTypeSnowflake, func() drivers.Driver {
	//	return &warehouses.SnowflakeDriver{} // 使用 olap 包中的 SnowflakeDriver 而不是 warehouses 包中的
	//})
	//dr.register(model.DatabaseTypeDatabricks, func() drivers.Driver {
	//	return &olap.DatabricksDriver{}
	//})
	//// Cloud Data Warehouses
	//dr.register(model.DatabaseTypeRedshift, func() drivers.Driver {
	//	return &warehouses.RedshiftDriver{}
	//})
	//dr.register(model.DatabaseTypeBigQuery, func() drivers.Driver {
	//	return &warehouses.BigQueryDriver{}
	//})

	// // Object Storage
	// dr.register(model.DatabaseTypeS3Parquet, func() drivers.Driver {
	// 	return &object_storage.S3ParquetDriver{}
	// })
	// dr.register(model.DatabaseTypeS3ORC, func() drivers.Driver {
	// 	return &object_storage.S3ORCDriver{}
	// })
	// dr.register(model.DatabaseTypeS3Avro, func() drivers.Driver {
	// 	return &object_storage.S3AvroDriver{}
	// })
	// dr.register(model.DatabaseTypeS3CSV, func() drivers.Driver {
	// 	return &object_storage.S3CSVDriver{}
	// })
	// dr.register(model.DatabaseTypeS3JSON, func() drivers.Driver {
	// 	return &object_storage.S3JSONDriver{}
	// })
	// dr.register(model.DatabaseTypeMinIOParquet, func() drivers.Driver {
	// 	return &object_storage.MinIOParquetDriver{}
	// })
	// dr.register(model.DatabaseTypeMinIOCSV, func() drivers.Driver {
	// 	return &object_storage.MinIOCSVDriver{}
	// })
	// dr.register(model.DatabaseTypeAlibabaOSSParquet, func() drivers.Driver {
	// 	return &object_storage.OSSParquetDriver{}
	// })
	// dr.register(model.DatabaseTypeTencentCOSParquet, func() drivers.Driver {
	// 	return &object_storage.COSParquetDriver{}
	// })
	// dr.register(model.DatabaseTypeAzureBlobParquet, func() drivers.Driver {
	// 	return &object_storage.AzureBlobParquetDriver{}
	// })
	// dr.register(model.DatabaseTypeOSSDelta, func() drivers.Driver {
	// 	return &object_storage.OSSDeltaDriver{}
	// })
	// dr.register(model.DatabaseTypeMinIOIceberg, func() drivers.Driver {
	// 	return &object_storage.MinIOIcebergDriver{}
	// })
	// dr.register(model.DatabaseTypeAzureDelta, func() drivers.Driver {
	// 	return &object_storage.AzureDeltaDriver{}
	// })
	// dr.register(model.DatabaseTypeAzureParquet, func() drivers.Driver {
	// 	return &object_storage.AzureParquetDriver{}
	// })

	// // // Distributed File Systems (these are already handled by object_storage package)
	// dr.register(model.DatabaseTypeHDFSAvro, func() drivers.Driver {
	// 	return &object_storage.HDFSAvroDriver{}
	// })
	// dr.register(model.DatabaseTypeHDFSParquet, func() drivers.Driver {
	// 	return &object_storage.HDFSParquetDriver{}
	// })
	// dr.register(model.DatabaseTypeHDFSCSV, func() drivers.Driver {
	// 	return &object_storage.HDFSCSVDriver{}
	// })
	// dr.register(model.DatabaseTypeHDFSText, func() drivers.Driver {
	// 	return &object_storage.HDFSTextDriver{}
	// })
	// dr.register(model.DatabaseTypeHDFSParquetCompressed, func() drivers.Driver {
	// 	return &object_storage.HDFSParquetCompressedDriver{}
	// })
	// dr.register(model.DatabaseTypeOzoneParquet, func() drivers.Driver {
	// 	return &object_storage.OzoneParquetDriver{}
	// })
	// dr.register(model.DatabaseTypeOzoneText, func() drivers.Driver {
	// 	return &object_storage.OzoneTextDriver{}
	// })
	// dr.register(model.DatabaseTypeOzoneAvro, func() drivers.Driver {
	// 	return &object_storage.OzoneAvroDriver{}
	// })
	// dr.register(model.DatabaseTypeMinIODelta, func() drivers.Driver {
	// 	return &object_storage.MinIODeltaDriver{}
	// })

	// OLAP Engines
	//dr.register(model.DatabaseTypeClickHouse, func() drivers.Driver {
	//	return &olap.ClickHouseDriver{}
	//})
	//dr.register(model.DatabaseTypeApacheDoris, func() drivers.Driver {
	//	return &olap.DorisDriver{}
	//})
	//dr.register(model.DatabaseTypeStarRocks, func() drivers.Driver {
	//	return &olap.StarRocksDriver{}
	//})
	//dr.register(model.DatabaseTypeApacheDruid, func() drivers.Driver {
	//	return &olap.DruidDriver{}
	//})
}

// register registers a driver factory function
func (dr *DriverRegistry) register(dbType model.DatabaseType, factory func() drivers.Driver) {
	dr.drivers[dbType] = factory
}

// RegisterDriver registers a concrete driver instance for a database type.
// This mirrors older code that allowed drivers to self-register and keeps
// backward compatibility with driver packages that call this method.
func (dr *DriverRegistry) RegisterDriver(dbType model.DatabaseType, driver drivers.Driver) {
	dr.mutex.Lock()
	defer dr.mutex.Unlock()
	dr.drivers[dbType] = func() drivers.Driver { return driver }
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
