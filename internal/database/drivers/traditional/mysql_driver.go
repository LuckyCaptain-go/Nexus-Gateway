package traditional

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"nexus-gateway/internal/database/drivers"
	"nexus-gateway/internal/model"
)

// MySQLDriver implements Driver for MySQL/MariaDB
type MySQLDriver struct{}

func (d *MySQLDriver) Open(dsn string) (*sql.DB, error) {
	fmt.Printf("Opening MySQL connection with DSN: %s\n", dsn)
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

func (d *MySQLDriver) GetCategory() drivers.DriverCategory {
	return drivers.CategoryRelational
}

func (d *MySQLDriver) GetCapabilities() drivers.DriverCapabilities {
	return drivers.DriverCapabilities{
		SupportsSQL:             true,
		SupportsTransaction:     true,
		SupportsSchemaDiscovery: true,
		SupportsTimeTravel:      false,
		RequiresTokenRotation:   false,
		SupportsStreaming:       true,
	}
}

func (d *MySQLDriver) ConfigureAuth(authConfig interface{}) error {
	// MySQL uses basic auth, no special configuration
	return nil
}
