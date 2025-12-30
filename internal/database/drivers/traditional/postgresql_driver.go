package traditional

import (
	"database/sql"
	"fmt"

	"nexus-gateway/internal/database"
	"nexus-gateway/internal/model"
)

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

func (d *PostgreSQLDriver) GetCategory() database.DriverCategory {
	return database.CategoryRelational
}

func (d *PostgreSQLDriver) GetCapabilities() database.DriverCapabilities {
	return database.DriverCapabilities{
		SupportsSQL:             true,
		SupportsTransaction:     true,
		SupportsSchemaDiscovery: true,
		SupportsTimeTravel:      false,
		RequiresTokenRotation:   false,
		SupportsStreaming:       true,
	}
}

func (d *PostgreSQLDriver) ConfigureAuth(authConfig interface{}) error {
	return nil
}
