package traditional

import (
	"database/sql"
	"fmt"
	"nexus-gateway/internal/database/drivers"
	"strings"

	"nexus-gateway/internal/model"
)

// OracleDriver implements Driver for Oracle
type OracleDriver struct{}

func (d *OracleDriver) ApplyBatchPagination(sql string, batchSize, offset int64) (string, error) {
	sql = strings.TrimSpace(sql)

	// Check if query already has LIMIT or OFFSET
	sqlUpper := strings.ToUpper(sql)
	if strings.Contains(sqlUpper, " LIMIT ") || strings.Contains(sqlUpper, " OFFSET ") {
		// For complex queries with existing pagination, add a subquery wrapper
		return fmt.Sprintf("SELECT * FROM (SELECT a.*, ROWNUM rnum FROM (%s) a WHERE ROWNUM <= %d) WHERE rnum > %d", sql, batchSize+offset, offset), nil

	}
	if offset > 0 {
		return fmt.Sprintf("SELECT * FROM (SELECT a.*, ROWNUM rnum FROM (%s) a WHERE ROWNUM <= %d) WHERE rnum > %d", sql, batchSize+offset, offset), nil
	}
	return fmt.Sprintf("SELECT * FROM (%s) WHERE ROWNUM <= %d", sql, batchSize), nil
}

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
	return fmt.Sprintf("oracle://%s:%s@%s:%d/%s",
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

func (d *OracleDriver) GetCategory() drivers.DriverCategory {
	return drivers.CategoryRelational
}

func (d *OracleDriver) GetCapabilities() drivers.DriverCapabilities {
	return drivers.DriverCapabilities{
		SupportsSQL:             true,
		SupportsTransaction:     true,
		SupportsSchemaDiscovery: true,
		SupportsTimeTravel:      false,
		RequiresTokenRotation:   false,
		SupportsStreaming:       true,
	}
}

func (d *OracleDriver) ConfigureAuth(authConfig interface{}) error {
	return nil
}
