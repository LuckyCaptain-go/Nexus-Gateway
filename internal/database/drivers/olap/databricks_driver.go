package olap

import (
	"database/sql"
	"fmt"
	"nexus-gateway/internal/database/drivers"
	"nexus-gateway/internal/model"
)

// DatabricksDriver implements the Driver interface for Databricks
type DatabricksDriver struct{}

func (d *DatabricksDriver) Open(dsn string) (*sql.DB, error) {
	return nil, fmt.Errorf("Databricks driver not yet implemented")
}

func (d *DatabricksDriver) ValidateDSN(dsn string) error {
	return fmt.Errorf("Databricks driver not yet implemented")
}

func (d *DatabricksDriver) GetDefaultPort() int {
	return 443
}

func (d *DatabricksDriver) BuildDSN(config *model.DataSourceConfig) string {
	return ""
}

func (d *DatabricksDriver) GetDatabaseTypeName() string {
	return "databricks"
}

func (d *DatabricksDriver) TestConnection(db *sql.DB) error {
	return fmt.Errorf("Databricks driver not yet implemented")
}

func (d *DatabricksDriver) GetDriverName() string {
	return "databricks"
}

func (d *DatabricksDriver) GetCategory() drivers.DriverCategory {
	return drivers.CategoryCloudDataWarehouse
}