package warehouses

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"nexus-gateway/internal/database"
	"nexus-gateway/internal/model"
)

// DatabricksDriver implements Driver interface for Databricks
type DatabricksDriver struct {
	restClient *DatabricksRESTClient
	poller     *DatabricksStatementPoller
	config     *DatabricksConfig
}

// DatabricksConfig holds Databricks configuration
type DatabricksConfig struct {
	WorkspaceURL string // Databricks workspace URL
	Token        string // Personal Access Token
	WarehouseID  string // SQL Warehouse ID
	HTTPPath     string // HTTP path (for JDBC)
	Catalog      string // Default catalog (optional)
	Schema       string // Default schema (optional)
}

// NewDatabricksDriver creates a new Databricks driver
func NewDatabricksDriver(config *DatabricksConfig) (*DatabricksDriver, error) {
	if config.WorkspaceURL == "" {
		return nil, fmt.Errorf("workspace URL is required")
	}
	if config.Token == "" {
		return nil, fmt.Errorf("access token is required")
	}
	if config.WarehouseID == "" {
		return nil, fmt.Errorf("warehouse ID is required")
	}

	restClient, err := NewDatabricksRESTClient(config.WorkspaceURL, config.Token, config.WarehouseID)
	if err != nil {
		return nil, err
	}

	return &DatabricksDriver{
		restClient: restClient,
		poller:     NewDatabricksStatementPoller(restClient),
		config:     config,
	}, nil
}

// Open opens a connection (not directly supported - use REST API)
func (d *DatabricksDriver) Open(dsn string) (*sql.DB, error) {
	return nil, fmt.Errorf("Databricks does not support direct database connections - use REST API or Simba JDBC driver")
}

// ValidateDSN validates the connection string
func (d *DatabricksDriver) ValidateDSN(dsn string) error {
	if dsn == "" {
		return fmt.Errorf("DSN cannot be empty")
	}
	return nil
}

// GetDefaultPort returns the default port
func (d *DatabricksDriver) GetDefaultPort() int {
	return 443 // HTTPS
}

// BuildDSN builds a connection string from configuration
func (d *DatabricksDriver) BuildDSN(config *model.DataSourceConfig) string {
	return fmt.Sprintf("databricks://%s?token=%s&warehouse=%s", config.Host, config.Password, config.Database)
}

// GetDatabaseTypeName returns the database type name
func (d *DatabricksDriver) GetDatabaseTypeName() string {
	return "databricks"
}

// TestConnection tests if the connection is working
func (d *DatabricksDriver) TestConnection(db *sql.DB) error {
	return fmt.Errorf("use TestConnectionREST instead for Databricks")
}

// TestConnectionREST tests REST API connectivity
func (d *DatabricksDriver) TestConnectionREST(ctx context.Context) error {
	// Try to get warehouse status
	_, err := d.restClient.GetWarehouseStatus(ctx, d.config.WarehouseID)
	return err
}

// GetDriverName returns the driver name
func (d *DatabricksDriver) GetDriverName() string {
	return "databricks-rest"
}

// GetCategory returns the driver category
func (d *DatabricksDriver) GetCategory() database.DriverCategory {
	return database.CategoryWarehouse
}

// GetCapabilities returns driver capabilities
func (d *DatabricksDriver) GetCapabilities() database.DriverCapabilities {
	return database.DriverCapabilities{
		SupportsSQL:             true,
		SupportsTransaction:     true, // ACID support via Delta Lake
		SupportsSchemaDiscovery: true,
		SupportsTimeTravel:      true, // Via Delta Lake time travel
		RequiresTokenRotation:   false,
		SupportsStreaming:       true,
	}
}

// ConfigureAuth configures authentication
func (d *DatabricksDriver) ConfigureAuth(authConfig interface{}) error {
	return nil
}

// =============================================================================
// Databricks-Specific Methods
// =============================================================================

// ExecuteQuery executes a SQL query via Databricks SQL Warehouse
func (d *DatabricksDriver) ExecuteQuery(ctx context.Context, sql string) (*DatabricksQueryResult, error) {
	return d.poller.ExecuteAndWait(ctx, sql)
}

// ExecuteQueryWithCallback executes query with status callback
func (d *DatabricksDriver) ExecuteQueryWithCallback(ctx context.Context, sql string, callback func(*DatabricksStatementExecution) error) (*DatabricksQueryResult, error) {
	execution, err := d.restClient.ExecuteStatement(ctx, sql)
	if err != nil {
		return nil, err
	}

	return d.poller.PollWithCallback(ctx, execution.StatementID, callback)
}

// GetWarehouseStatus retrieves warehouse status
func (d *DatabricksDriver) GetWarehouseStatus(ctx context.Context) (*DatabricksWarehouseStatus, error) {
	return d.restClient.GetWarehouseStatus(ctx, d.config.WarehouseID)
}

// StartWarehouse starts the SQL warehouse
func (d *DatabricksDriver) StartWarehouse(ctx context.Context) error {
	return d.restClient.StartWarehouse(ctx, d.config.WarehouseID)
}

// StopWarehouse stops the SQL warehouse
func (d *DatabricksDriver) StopWarehouse(ctx context.Context) error {
	return d.restClient.StopWarehouse(ctx, d.config.WarehouseID)
}

// ListWarehouses lists all available warehouses
func (d *DatabricksDriver) ListWarehouses(ctx context.Context) ([]DatabricksWarehouse, error) {
	return d.restClient.ListWarehouses(ctx)
}

// GetCatalogs lists available catalogs (databases)
func (d *DatabricksDriver) GetCatalogs(ctx context.Context) ([]DatabricksDatabase, error) {
	// Use SHOW CATALOGS command
	result, err := d.ExecuteQuery(ctx, "SHOW CATALOGS")
	if err != nil {
		return nil, err
	}

	var databases []DatabricksDatabase
	for _, row := range result.Data {
		if len(row) > 0 {
			if catalog, ok := row[0].(string); ok {
				databases = append(databases, DatabricksDatabase{
					CatalogName: catalog,
				})
			}
		}
	}

	return databases, nil
}

// GetSchemas lists schemas in a catalog
func (d *DatabricksDriver) GetSchemas(ctx context.Context, catalog string) ([]DatabricksSchema, error) {
	sql := fmt.Sprintf("SHOW SCHEMAS IN %s", catalog)
	result, err := d.ExecuteQuery(ctx, sql)
	if err != nil {
		return nil, err
	}

	var schemas []DatabricksSchema
	for _, row := range result.Data {
		if len(row) > 0 {
			schemas = append(schemas, DatabricksSchema{
				CatalogName: catalog,
				Name:        fmt.Sprintf("%v", row[0]),
			})
		}
	}

	return schemas, nil
}

// GetTables lists tables in a catalog and schema
func (d *DatabricksDriver) GetTables(ctx context.Context, catalog, schema string) ([]DatabricksTable, error) {
	sql := fmt.Sprintf("SHOW TABLES IN %s.%s", catalog, schema)
	result, err := d.ExecuteQuery(ctx, sql)
	if err != nil {
		return nil, err
	}

	var tables []DatabricksTable
	for _, row := range result.Data {
		if len(row) >= 2 {
			tables = append(tables, DatabricksTable{
				CatalogName: catalog,
				SchemaName:  schema,
				Name:        fmt.Sprintf("%v", row[0]),
				TableType:   fmt.Sprintf("%v", row[1]),
			})
		}
	}

	return tables, nil
}

// QueryDeltaTimeTravel performs time travel query on Delta Lake table
func (d *DatabricksDriver) QueryDeltaTimeTravel(ctx context.Context, table, versionOrTimestamp string) (*DatabricksQueryResult, error) {
	// Databricks Delta Lake time travel syntax
	sql := fmt.Sprintf("SELECT * FROM %s VERSION AS OF %s", table, versionOrTimestamp)
	return d.ExecuteQuery(ctx, sql)
}

// RegisterDatabricksDriver registers the Databricks driver globally
func RegisterDatabricksDriver(config *DatabricksConfig) error {
	driver, err := NewDatabricksDriver(config)
	if err != nil {
		return err
	}

	database.GetDriverRegistry().RegisterDriver(model.DatabaseTypeDatabricks, driver)
	return nil
}

// GetClusterInfo retrieves cluster information
func (d *DatabricksDriver) GetClusterInfo(ctx context.Context, clusterID string) (*DatabricksClusterInfo, error) {
	// Would use Clusters API
	return &DatabricksClusterInfo{
		ClusterID: clusterID,
	}, nil
}

// DatabricksClusterInfo contains cluster information
type DatabricksClusterInfo struct {
	ClusterID       string
	SparkVersion    string
	DriverNodeType  string
	WorkerNodeType  string
	NumWorkers      int
	AutoScale       bool
	AutoScaleMin    int
	AutoScaleMax    int
}

// CreateWarehouse creates a new SQL warehouse
func (d *DatabricksDriver) CreateWarehouse(ctx context.Context, warehouse *DatabricksWarehouse) error {
	// Would use Warehouses API
	return fmt.Errorf("warehouse creation not yet implemented")
}

// DeleteWarehouse deletes a SQL warehouse
func (d *DatabricksDriver) DeleteWarehouse(ctx context.Context, warehouseID string) error {
	// Would use Warehouses API
	return fmt.Errorf("warehouse deletion not yet implemented")
}

// EditWarehouse modifies warehouse configuration
func (d *DatabricksDriver) EditWarehouse(ctx context.Context, warehouseID string, updates map[string]interface{}) error {
	return fmt.Errorf("warehouse modification not yet implemented")
}

// CancelStatement cancels a running statement
func (d *DatabricksDriver) CancelStatement(ctx context.Context, statementID string) error {
	return d.poller.CancelAndWait(ctx, statementID)
}

// GetStatementHistory retrieves statement history
func (d *DatabricksDriver) GetStatementHistory(ctx context.Context, limit int) ([]DatabricksStatementHistory, error) {
	sql := fmt.Sprintf("SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(LIMIT %d)) ORDER BY start_time DESC", limit)
	result, err := d.ExecuteQuery(ctx, sql)
	if err != nil {
		return nil, err
	}

	// Parse results
	var history []DatabricksStatementHistory
	for _, row := range result.Data {
		if len(row) >= 6 {
			history = append(history, DatabricksStatementHistory{
				QueryID:    fmt.Sprintf("%v", row[0]),
				QueryText:  fmt.Sprintf("%v", row[1]),
				Status:     fmt.Sprintf("%v", row[2]),
				StartTime:  parseTime(fmt.Sprintf("%v", row[3])),
				EndTime:    parseTime(fmt.Sprintf("%v", row[4])),
				DurationMs: parseInt(fmt.Sprintf("%v", row[5])),
			})
		}
	}

	return history, nil
}

// DatabricksStatementHistory contains statement history
type DatabricksStatementHistory struct {
	QueryID    string
	QueryText  string
	Status     string
	StartTime  time.Time
	EndTime    time.Time
	DurationMs int64
}

// Helper functions
func parseTime(s string) time.Time {
	// Parse Databricks timestamp format
	t, _ := time.Parse(time.RFC3339, s)
	return t
}

func parseInt(s string) int64 {
	var i int64
	fmt.Sscanf(s, "%d", &i)
	return i
}

// GetNotebookStatus retrieves notebook execution status
func (d *DatabricksDriver) GetNotebookStatus(ctx context.Context, notebookID, runID string) (*DatabricksNotebookStatus, error) {
	// Would use Jobs API or Workspace API
	return &DatabricksNotebookStatus{
		NotebookID: notebookID,
		RunID:      runID,
	}, nil
}

// DatabricksNotebookStatus contains notebook status
type DatabricksNotebookStatus struct {
	NotebookID string
	RunID      string
	State      string
}

// ExecuteNotebook executes a notebook
func (d *DatabricksDriver) ExecuteNotebook(ctx context.Context, notebookID string) (string, error) {
	// Would use Jobs API to create and run a job
	return "", fmt.Errorf("notebook execution not yet implemented")
}

// CancelNotebookRun cancels a running notebook
func (d *DatabricksDriver) CancelNotebookRun(ctx context.Context, runID string) error {
	return fmt.Errorf("notebook cancellation not yet implemented")
}
