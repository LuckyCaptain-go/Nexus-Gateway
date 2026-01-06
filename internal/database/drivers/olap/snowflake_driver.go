package olap

import (
	"database/sql"
	"fmt"
	"nexus-gateway/internal/database/drivers"
	"nexus-gateway/internal/model"
)

// SnowflakeDriver implements the Driver interface for Snowflake
type SnowflakeDriver struct{}

func (d *SnowflakeDriver) Open(dsn string) (*sql.DB, error) {
	return nil, fmt.Errorf("Snowflake driver not yet implemented")
}

func (d *SnowflakeDriver) ValidateDSN(dsn string) error {
	return fmt.Errorf("Snowflake driver not yet implemented")
}

func (d *SnowflakeDriver) GetDefaultPort() int {
	return 443
}

func (d *SnowflakeDriver) BuildDSN(config *model.DataSourceConfig) string {
	return ""
}

func (d *SnowflakeDriver) GetDatabaseTypeName() string {
	return "snowflake"
}

func (d *SnowflakeDriver) TestConnection(db *sql.DB) error {
	return fmt.Errorf("Snowflake driver not yet implemented")
}

func (d *SnowflakeDriver) ConfigureAuth(authConfig interface{}) error {
	return nil
}

func (d *SnowflakeDriver) GetDriverName() string {
	return "snowflake"
}

func (d *SnowflakeDriver) GetCategory() drivers.DriverCategory {
	return drivers.CategoryCloudDataWarehouse
}
