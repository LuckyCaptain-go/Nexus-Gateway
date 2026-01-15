package warehouses

import (
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	"nexus-gateway/internal/database/drivers"
	"nexus-gateway/internal/model"

	_ "sqlflow.org/gohive"
)

// HiveDriver implements the Driver interface for Apache Hive
type HiveDriver struct {
	base *drivers.DriverBase
}

// NewHiveDriver creates a new Hive driver instance
func NewHiveDriver() *HiveDriver {
	return &HiveDriver{
		base: drivers.NewDriverBase(model.DatabaseTypeApacheHive, drivers.CategoryCloudDataWarehouse),
	}
}

// ApplyBatchPagination applies pagination to SQL query for batch processing
func (hd *HiveDriver) ApplyBatchPagination(sql string, batchSize, offset int64) (string, error) {
	return fmt.Sprintf("%s LIMIT %d OFFSET %d", sql, batchSize, offset), nil
}

// Open opens a connection to Hive database
func (hd *HiveDriver) Open(dsn string) (*sql.DB, error) {
	// 使用gohive驱动连接到Hive
	// gohive使用标准的database/sql接口
	db, err := sql.Open("gohive", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Hive: %v", err)
	}

	return db, nil
}

// ValidateDSN validates the Hive connection string
func (hd *HiveDriver) ValidateDSN(dsn string) error {
	// Parse DSN and validate its format
	parsedURL, err := url.Parse(dsn)
	if err != nil {
		return fmt.Errorf("invalid DSN format: %v", err)
	}
	
	// Check if it's using hive or hiveserver2 scheme
	scheme := strings.ToLower(parsedURL.Scheme)
	if scheme != "hive" && scheme != "hiveserver2" {
		return fmt.Errorf("invalid scheme in DSN: %s, expected 'hive' or 'hiveserver2'", scheme)
	}
	
	if parsedURL.Host == "" {
		return fmt.Errorf("missing host in DSN")
	}
	
	if parsedURL.User == nil {
		return fmt.Errorf("missing username in DSN")
	}
	
	password, ok := parsedURL.User.Password()
	if !ok {
		return fmt.Errorf("missing password in DSN")
	}
	
	if password == "" {
		return fmt.Errorf("empty password in DSN")
	}
	
	return nil
}

// GetDefaultPort returns the default port for Hive
func (hd *HiveDriver) GetDefaultPort() int {
	return 10000 // Default HiveServer2 port
}

// BuildDSN builds a Hive connection string from configuration
func (hd *HiveDriver) BuildDSN(config *model.DataSourceConfig) string {
	// 构建Hive连接字符串
	// gohive expects format like: host:port/database?auth=PLAIN
	dsn := fmt.Sprintf("hiveserver2://%s:%s@%s:%d/%s",
		config.Username,
		url.QueryEscape(config.Password),
		config.Host,
		config.Port,
		config.Database,
	)
	
	// 添加额外的配置参数
	if config.AdditionalProps != nil {
		params := url.Values{}
		for k, v := range config.AdditionalProps {
			params.Add(k, fmt.Sprintf("%v", v))
		}
		if params.Encode() != "" {
			dsn += "?" + params.Encode()
		}
	}
	
	return dsn
}

// GetDatabaseTypeName returns the database type name
func (hd *HiveDriver) GetDatabaseTypeName() string {
	return hd.base.GetDatabaseTypeName()
}

// TestConnection tests if the Hive connection is working
func (hd *HiveDriver) TestConnection(db *sql.DB) error {
	// Execute a simple query to test the connection
	query := "SELECT 1"
	row := db.QueryRow(query)
	var result int
	err := row.Scan(&result)
	if err != nil {
		return fmt.Errorf("failed to execute test query: %v", err)
	}
	
	if result != 1 {
		return fmt.Errorf("unexpected result from test query: %d", result)
	}
	
	return nil
}

// GetDriverName returns the underlying SQL driver name for Hive
func (hd *HiveDriver) GetDriverName() string {
	return "gohive"
}

// GetCategory returns the driver category
func (hd *HiveDriver) GetCategory() drivers.DriverCategory {
	return hd.base.GetCategory()
}

// GetCapabilities returns driver capabilities
func (hd *HiveDriver) GetCapabilities() drivers.DriverCapabilities {
	return drivers.DriverCapabilities{
		SupportsSQL:             true,
		SupportsTransaction:     false, // Hive doesn't support ACID transactions in the same way as traditional RDBMS
		SupportsSchemaDiscovery: true,
		SupportsTimeTravel:      false,
		RequiresTokenRotation:   false,
		SupportsStreaming:       true, // Supports streaming of large result sets
	}
}

// ConfigureAuth configures authentication for Hive (supports various auth mechanisms)
func (hd *HiveDriver) ConfigureAuth(authConfig interface{}) error {
	// Hive supports various authentication methods like LDAP, Kerberos, etc.
	// Implementation would depend on specific authConfig provided
	// This is a placeholder for future extension
	
	return nil
}