package olap

import (
	"context"
	"database/sql"
	"fmt"
	"nexus-gateway/internal/database/drivers"
	"time"

	"nexus-gateway/internal/database"
	"nexus-gateway/internal/model"
)

// DruidDriver implements Driver interface for Apache Druid
type DruidDriver struct {
	config     *DruidConfig
	restClient *DruidRESTClient
}

// DruidConfig holds Druid configuration
type DruidConfig struct {
	BrokerURL      string
	CoordinatorURL string
	OverlordURL    string
	Username       string
	Password       string
	Timeout        time.Duration
	UseSQL         bool // Use SQL API or native query
}

// NewDruidDriver creates a new Druid driver
func NewDruidDriver(config *DruidConfig) (*DruidDriver, error) {
	if config.BrokerURL == "" {
		return nil, fmt.Errorf("broker URL is required")
	}

	restClient := NewDruidRESTClient(config.BrokerURL)

	return &DruidDriver{
		config:     config,
		restClient: restClient,
	}, nil
}

// Open opens a connection (not applicable for Druid)
func (d *DruidDriver) Open(dsn string) (*sql.DB, error) {
	return nil, fmt.Errorf("Druid driver uses REST API, not standard database connections")
}

// ValidateDSN validates the connection string
func (d *DruidDriver) ValidateDSN(dsn string) error {
	if dsn == "" {
		return fmt.Errorf("DSN cannot be empty")
	}
	return nil
}

// GetDefaultPort returns the default Druid port
func (d *DruidDriver) GetDefaultPort() int {
	return 8082 // Broker port
}

// BuildDSN builds a connection string from configuration
func (d *DruidDriver) BuildDSN(config *model.DataSourceConfig) string {
	return fmt.Sprintf("http://%s:%d", config.Host, config.Port)
}

// GetDatabaseTypeName returns the database type name
func (d *DruidDriver) GetDatabaseTypeName() string {
	return "druid"
}

// TestConnection tests if the connection is working
func (d *DruidDriver) TestConnection(db *sql.DB) error {
	return fmt.Errorf("use TestConnectionContext instead")
}

// TestConnectionContext tests the Druid connection
func (d *DruidDriver) TestConnectionContext(ctx context.Context) error {
	status, err := d.restClient.GetClusterStatus(ctx)
	if err != nil {
		return fmt.Errorf("failed to get cluster status: %w", err)
	}

	if status.Version == "" {
		return fmt.Errorf("invalid cluster status")
	}

	return nil
}

// GetDriverName returns the driver name
func (d *DruidDriver) GetDriverName() string {
	return "druid-rest"
}

// GetCategory returns the driver category
func (d *DruidDriver) GetCategory() drivers.DriverCategory {
	return drivers.CategoryOLAP
}

// GetCapabilities returns driver capabilities
func (d *DruidDriver) GetCapabilities() drivers.DriverCapabilities {
	return drivers.DriverCapabilities{
		SupportsSQL:             true,
		SupportsTransaction:     false,
		SupportsSchemaDiscovery: true,
		SupportsTimeTravel:      true,
		RequiresTokenRotation:   false,
		SupportsStreaming:       false,
	}
}

// ConfigureAuth configures authentication
func (d *DruidDriver) ConfigureAuth(authConfig interface{}) error {
	return nil
}

// ExecuteSQL executes a SQL query via Druid SQL API
func (d *DruidDriver) ExecuteSQL(ctx context.Context, sql string) (*DruidQueryResult, error) {
	return d.restClient.ExecuteSQLQuery(ctx, sql)
}

// GetDatasources retrieves list of datasources
func (d *DruidDriver) GetDatasources(ctx context.Context) ([]string, error) {
	return d.restClient.GetDatasources(ctx)
}

// GetDatasourceMetadata retrieves metadata for a datasource
func (d *DruidDriver) GetDatasourceMetadata(ctx context.Context, datasource string) (*DruidDatasourceMetadata, error) {
	return d.restClient.GetDatasourceMetadata(ctx, datasource)
}

// ExecuteTimeTravelQuery executes a time-travel query
func (d *DruidDriver) ExecuteTimeTravelQuery(ctx context.Context, sql string, timestamp string) (*DruidQueryResult, error) {
	// Druid uses intervals for time travel
	timeTravelSQL := fmt.Sprintf("%s WHERE __time >= TIMESTAMP '%s' AND __time < TIMESTAMP '%s' + INTERVAL 1 DAY",
		sql, timestamp, timestamp)

	return d.restClient.ExecuteSQLQuery(ctx, timeTravelSQL)
}

// GetClusterInfo retrieves cluster information
func (d *DruidDriver) GetClusterInfo(ctx context.Context) (*DruidClusterInfo, error) {
	status, err := d.restClient.GetClusterStatus(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster status: %w", err)
	}

	info := &DruidClusterInfo{
		Version:        status.Version,
		BrokerURL:      d.config.BrokerURL,
		CoordinatorURL: d.config.CoordinatorURL,
		OverlordURL:    d.config.OverlordURL,
	}

	return info, nil
}

// DruidClusterInfo represents cluster information
type DruidClusterInfo struct {
	Version        string
	BrokerURL      string
	CoordinatorURL string
	OverlordURL    string
}

// RegisterDruidDriver registers the Druid driver globally
func RegisterDruidDriver(config *DruidConfig) error {
	driver, err := NewDruidDriver(config)
	if err != nil {
		return err
	}

	database.GetDriverRegistry().RegisterDriver(model.DatabaseTypeDruid, driver)
	return nil
}
