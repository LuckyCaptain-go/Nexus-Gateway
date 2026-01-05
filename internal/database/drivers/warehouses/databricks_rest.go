package warehouses

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// DatabricksRESTClient implements REST API client for Databricks
type DatabricksRESTClient struct {
	baseURL     string // Databricks workspace URL
	httpClient  *http.Client
	token       string // Personal Access Token
	warehouseID string // SQL Warehouse ID
}

// NewDatabricksRESTClient creates a new Databricks REST client
func NewDatabricksRESTClient(workspaceURL, token, warehouseID string) (*DatabricksRESTClient, error) {
	if workspaceURL == "" {
		return nil, fmt.Errorf("workspace URL is required")
	}
	if token == "" {
		return nil, fmt.Errorf("access token is required")
	}

	return &DatabricksRESTClient{
		baseURL:     workspaceURL,
		httpClient:  &http.Client{Timeout: 120 * time.Second},
		token:       token,
		warehouseID: warehouseID,
	}, nil
}

// ExecuteStatement executes a SQL statement
func (c *DatabricksRESTClient) ExecuteStatement(ctx context.Context, sql string) (*DatabricksStatementExecution, error) {
	url := fmt.Sprintf("%s/api/2.0/sql/statements", c.baseURL)

	payload := DatabricksStatementRequest{
		Statement:   sql,
		WarehouseID: c.warehouseID,
		WaitTimeout: "50s",
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	c.setAuthHeader(req)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	var execution DatabricksStatementExecution
	if err := json.NewDecoder(resp.Body).Decode(&execution); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &execution, nil
}

// GetStatementStatus retrieves statement execution status
func (c *DatabricksRESTClient) GetStatementStatus(ctx context.Context, statementID string) (*DatabricksStatementExecution, error) {
	url := fmt.Sprintf("%s/api/2.0/sql/statements/%s", c.baseURL, statementID)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	c.setAuthHeader(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	var execution DatabricksStatementExecution
	if err := json.NewDecoder(resp.Body).Decode(&execution); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &execution, nil
}

// GetStatementResults retrieves statement results
func (c *DatabricksRESTClient) GetStatementResults(ctx context.Context, statementID string, offset int64) (*DatabricksQueryResult, error) {
	url := fmt.Sprintf("%s/api/2.0/sql/statements/%s/result", c.baseURL, statementID)

	if offset > 0 {
		url += fmt.Sprintf("?row_limit=%d&row_offset=%d", 10000, offset)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	c.setAuthHeader(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	var result DatabricksQueryResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

// CancelStatement cancels a running statement
func (c *DatabricksRESTClient) CancelStatement(ctx context.Context, statementID string) error {
	url := fmt.Sprintf("%s/api/2.0/sql/statements/%s/cancel", c.baseURL, statementID)

	req, err := http.NewRequestWithContext(ctx, "POST", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	c.setAuthHeader(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// ListWarehouses lists available SQL warehouses
func (c *DatabricksRESTClient) ListWarehouses(ctx context.Context) ([]DatabricksWarehouse, error) {
	url := fmt.Sprintf("%s/api/2.0/sql/warehouses", c.baseURL)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	c.setAuthHeader(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Warehouses []DatabricksWarehouse `json:"warehouses"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return result.Warehouses, nil
}

// GetWarehouseStatus retrieves warehouse status
func (c *DatabricksRESTClient) GetWarehouseStatus(ctx context.Context, warehouseID string) (*DatabricksWarehouseStatus, error) {
	url := fmt.Sprintf("%s/api/2.0/sql/warehouses/%s", c.baseURL, warehouseID)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	c.setAuthHeader(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	var status DatabricksWarehouseStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &status, nil
}

// StartWarehouse starts a SQL warehouse
func (c *DatabricksRESTClient) StartWarehouse(ctx context.Context, warehouseID string) error {
	url := fmt.Sprintf("%s/api/2.0/sql/warehouses/%s/start", c.baseURL, warehouseID)

	req, err := http.NewRequestWithContext(ctx, "POST", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	c.setAuthHeader(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// StopWarehouse stops a SQL warehouse
func (c *DatabricksRESTClient) StopWarehouse(ctx context.Context, warehouseID string) error {
	url := fmt.Sprintf("%s/api/2.0/sql/warehouses/%s/stop", c.baseURL, warehouseID)

	req, err := http.NewRequestWithContext(ctx, "POST", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	c.setAuthHeader(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// setAuthHeader sets the Bearer token authorization header
func (c *DatabricksRESTClient) setAuthHeader(req *http.Request) {
	req.Header.Set("Authorization", "Bearer "+c.token)
}

// =============================================================================
// Databricks Data Structures
// =============================================================================

// DatabricksStatementRequest represents a statement request
type DatabricksStatementRequest struct {
	Statement   string `json:"statement"`
	WarehouseID string `json:"warehouse_id"`
	WaitTimeout string `json:"wait_timeout,omitempty"`
	// Additional parameters
	Parameters map[string]interface{} `json:"parameters,omitempty"`
	Catalog    string                 `json:"catalog,omitempty"`
	Schema     string                 `json:"schema,omitempty"`
}

// DatabricksStatementExecution represents statement execution response
type DatabricksStatementExecution struct {
	StatementID string                    `json:"statement_id"`
	Status      DatabricksStatementStatus `json:"status"`
	ResultSet   *DatabricksQueryResult    `json:"result,omitempty"`
	Manifest    *DatabricksResultManifest `json:"manifest,omitempty"`
}

// DatabricksStatementStatus represents statement status
type DatabricksStatementStatus struct {
	State string           `json:"state"` // PENDING, RUNNING, SUCCEEDED, FAILED, CANCELED
	Error *DatabricksError `json:"error,omitempty"`
}

// DatabricksQueryResult represents query results
type DatabricksQueryResult struct {
	Data   [][]interface{}        `json:"data"`
	Schema DatabricksResultSchema `json:"schema"`
}

// DatabricksResultManifest represents result manifest
type DatabricksResultManifest struct {
	Schema   DatabricksResultSchema `json:"schema"`
	IsStaged bool                   `json:"is_staged"`
}

// DatabricksResultSchema represents result schema
type DatabricksResultSchema struct {
	Columns []DatabricksColumn `json:"columns"`
}

// DatabricksColumn represents a column definition
type DatabricksColumn struct {
	Name     string                 `json:"name"`
	Type     string                 `json:"type"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// DatabricksWarehouse represents a SQL warehouse
type DatabricksWarehouse struct {
	ID                      string `json:"id"`
	Name                    string `json:"name"`
	ClusterID               string `json:"cluster_id"`
	Size                    string `json:"size"` // SMALL, MEDIUM, LARGE, XLARGE, XXLARGE
	AutoStop                int    `json:"auto_stop_min"`
	EnableServerlessCompute bool   `json:"enable_serverless_compute"`
}

// DatabricksWarehouseStatus represents warehouse status
type DatabricksWarehouseStatus struct {
	ID       string           `json:"id"`
	Name     string           `json:"name"`
	Size     string           `json:"size"`
	State    string           `json:"state"` // RUNNING, STARTING, STOPPING, STOPPED
	AutoStop int              `json:"auto_stop_min"`
	Clusters []APIClusterInfo `json:"clusters"`
}

// APIClusterInfo contains cluster information from API response
type APIClusterInfo struct {
	ClusterID    string `json:"cluster_id"`
	JDBCPort     int    `json:"jdbc_port"`
	NumExecutors int    `json:"num_executors"`
}

// DatabricksError represents an error response
type DatabricksError struct {
	Message   string `json:"message"`
	ErrorCode string `json:"error_code"`
}

// DatabricksDatabase represents a database (catalog)
type DatabricksDatabase struct {
	CatalogName string `json:"catalog_name"`
	Name        string `json:"name"`
}

// DatabricksTable represents a table
type DatabricksTable struct {
	CatalogName string `json:"catalog_name"`
	SchemaName  string `json:"schema_name"`
	Name        string `json:"name"`
	TableType   string `json:"table_type"`
}

// DatabricksSchema represents schema information
type DatabricksSchema struct {
	CatalogName string `json:"catalog_name"`
	Name        string `json:"name"`
}

// GetClusterJDBCURL returns JDBC URL for a specific cluster
func (c *DatabricksRESTClient) GetClusterJDBCURL(clusterID, host, port, httpPath string) string {
	return fmt.Sprintf("jdbc:databricks://%s:%s/%s;transportMode=http;ssl=1;httpPath=%s;AuthMech=3;UID=token;PWD=%s",
		host, port, clusterID, httpPath, c.token)
}

// ExecuteCommand executes a Databricks command
func (c *DatabricksRESTClient) ExecuteCommand(ctx context.Context, clusterID, commandID string, parameters map[string]string) (*DatabricksCommandExecution, error) {
	// Commands API
	url := fmt.Sprintf("%s/api/2.0/commands/execute", c.baseURL)

	payload := map[string]interface{}{
		"clusterId":  clusterID,
		"language":   "sql",
		"commandId":  commandID,
		"parameters": parameters,
		"context":    map[string]interface{}{},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	c.setAuthHeader(req)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	var execution DatabricksCommandExecution
	if err := json.NewDecoder(resp.Body).Decode(&execution); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &execution, nil
}

// DatabricksCommandExecution represents command execution response
type DatabricksCommandExecution struct {
	ID      string                   `json:"id"`
	Status  string                   `json:"status"`
	Results *DatabricksCommandResult `json:"results,omitempty"`
}

// DatabricksCommandResult represents command results
type DatabricksCommandResult struct {
	Data   [][]interface{}        `json:"data"`
	Schema DatabricksResultSchema `json:"schema"`
}

// CancelCommand cancels a running command
func (c *DatabricksRESTClient) CancelCommand(ctx context.Context, commandID string) error {
	url := fmt.Sprintf("%s/api/2.0/commands/cancel", c.baseURL)

	payload := map[string]string{
		"commandId": commandID,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	c.setAuthHeader(req)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// GetCommandStatus retrieves command status
func (c *DatabricksRESTClient) GetCommandStatus(ctx context.Context, commandID string) (*DatabricksCommandExecution, error) {
	url := fmt.Sprintf("%s/api/2.0/commands/status", c.baseURL)

	payload := map[string]string{
		"commandId": commandID,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	c.setAuthHeader(req)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status %d: %s", resp.StatusCode, string(body))
	}

	var execution DatabricksCommandExecution
	if err := json.NewDecoder(resp.Body).Decode(&execution); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &execution, nil
}
