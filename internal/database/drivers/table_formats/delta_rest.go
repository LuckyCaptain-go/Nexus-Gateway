package table_formats

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"nexus-gateway/internal/model"
)

// DeltaRESTClient implements a REST client for Delta Lake
type DeltaRESTClient struct {
	baseURL    string
	httpClient *http.Client
	auth       *DeltaAuth
}

// DeltaAuth holds authentication credentials for Delta Lake API
type DeltaAuth struct {
	// Databricks uses personal access tokens
	Token     string
	TokenType string // "bearer", "databricks-token"
}

// DeltaConfig holds configuration for Delta Lake REST client
type DeltaConfig struct {
	BaseURL string        // Delta Lake REST endpoint (e.g., Databricks workspace)
	Timeout time.Duration // Request timeout
	Auth    *DeltaAuth    // Authentication credentials
}

// NewDeltaRESTClient creates a new Delta Lake REST client
func NewDeltaRESTClient(config *DeltaConfig) (*DeltaRESTClient, error) {
	if config.BaseURL == "" {
		return nil, fmt.Errorf("base URL is required")
	}

	timeout := config.Timeout
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	return &DeltaRESTClient{
		baseURL: config.BaseURL,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		auth: config.Auth,
	}, nil
}

// ListTables lists all tables in the workspace
func (c *DeltaRESTClient) ListTables(ctx context.Context, catalog, schema string) ([]DeltaTableIdentifier, error) {
	// Databricks API: GET /api/2.1/unity-catalog/tables
	url := fmt.Sprintf("%s/api/2.1/unity-catalog/tables", c.baseURL)

	if catalog != "" || schema != "" {
		// Add filters for catalog and schema
		params := ""
		if catalog != "" {
			params += fmt.Sprintf("catalog_name=%s", catalog)
		}
		if schema != "" {
			if params != "" {
				params += "&"
			}
			params += fmt.Sprintf("schema_name=%s", schema)
		}
		if params != "" {
			url += "?" + params
		}
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
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Tables []DeltaTableIdentifier `json:"tables"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return result.Tables, nil
}

// GetTableDetails retrieves table details
func (c *DeltaRESTClient) GetTableDetails(ctx context.Context, catalog, schema, table string) (*DeltaTableDetails, error) {
	// Databricks API: GET /api/2.1/unity-catalog/tables/{full_name}
	fullName := fmt.Sprintf("%s.%s.%s", catalog, schema, table)
	url := fmt.Sprintf("%s/api/2.1/unity-catalog/tables/%s", c.baseURL, fullName)

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
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	var details DeltaTableDetails
	if err := json.NewDecoder(resp.Body).Decode(&details); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &details, nil
}

// QueryTable executes a SQL query (via Databricks SQL endpoint)
func (c *DeltaRESTClient) QueryTable(ctx context.Context, warehouseID, sql string) (*DeltaQueryResult, error) {
	// Databricks SQL Warehouse API
	url := fmt.Sprintf("%s/api/2.0/sql/warehouses/%s/statements", c.baseURL, warehouseID)

	payload := map[string]interface{}{
		"statement": sql,
		"wait_timeout": "50s",
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
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
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	var result DeltaQueryResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

// GetTableHistory retrieves table history (for time travel)
func (c *DeltaRESTClient) GetTableHistory(ctx context.Context, tablePath string) ([]DeltaCommitInfo, error) {
	// Delta Lake history API (requires Databricks or Delta Standalone Reader)
	// This is a placeholder implementation
	return []DeltaCommitInfo{}, nil
}

// setAuthHeader sets the authentication header
func (c *DeltaRESTClient) setAuthHeader(req *http.Request) {
	if c.auth == nil {
		return
	}

	switch c.auth.TokenType {
	case "bearer", "databricks-token":
		req.Header.Set("Authorization", "Bearer "+c.auth.Token)
	default:
		req.Header.Set("Authorization", c.auth.Token)
	}
}

// =============================================================================
// Delta Lake Data Structures
// =============================================================================

// DeltaTableIdentifier identifies a Delta table
type DeltaTableIdentifier struct {
	CatalogName string `json:"catalog_name"`
	SchemaName  string `json:"schema_name"`
	Name        string `json:"name"`
	TableType   string `json:"table_type,omitempty"`
}

// DeltaTableDetails represents table details
type DeltaTableDetails struct {
	DeltaTableIdentifier
	Comment          string                 `json:"comment,omitempty"`
	CreatedAt        int64                  `json:"created_at"`
	CreatedBy        string                 `json:"created_by"`
	Properties       map[string]string      `json:"properties,omitempty"`
	Columns          []DeltaColumn          `json:"columns"`
	StorageLocation  string                 `json:"storage_location"`
	Format           DeltaFormat            `json:"format"`
}

// DeltaColumn represents a column definition
type DeltaColumn struct {
	Name           string `json:"name"`
	TypeText       string `json:"type_text"`
	TypeQualifiedName string `json:"type_name,omitempty"`
	Nullable        bool   `json:"nullable"`
	Comment        string `json:"comment,omitempty"`
}

// DeltaFormat represents table format
type DeltaFormat struct {
	Name string `json:"name"` // "delta"
}

// DeltaQueryResult represents query results
type DeltaQueryResult struct {
	StatementID string                 `json:"statement_id"`
	Status      DeltaStatementStatus   `json:"status"`
	Manifest    DeltaResultManifest    `json:"result"`
}

// DeltaStatementStatus represents statement status
type DeltaStatementStatus struct {
	State string `json:"state"` // "pending", "running", "succeeded", "failed"
}

// DeltaResultManifest represents result manifest
type DeltaResultManifest struct {
	Schema   DeltaResultSchema `json:"schema"`
	Data     [][]interface{}  `json:"data"`
}

// DeltaResultSchema represents result schema
type DeltaResultSchema struct {
	Columns []DeltaResultColumn `json:"columns"`
}

// DeltaResultColumn represents a result column
type DeltaResultColumn struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// DeltaCommitInfo represents a commit in Delta log
type DeltaCommitInfo struct {
	Version     int64     `json:"version"`
	Timestamp   time.Time `json:"timestamp"`
	UserId      string    `json:"user_id"`
	UserName    string    `json:"user_name"`
	Operation   string    `json:"operation"`
	OperationParameters map[string]interface{} `json:"operation_parameters,omitempty"`
	Note        string    `json:"note,omitempty"`
}

// ConvertDeltaTypeToStandardType converts Delta Lake types to standard types
func ConvertDeltaTypeToStandardType(deltaType string) string {
	switch deltaType {
	case "boolean":
		return "BOOLEAN"
	case "tinyint", "smallint", "int":
		return "INTEGER"
	case "bigint":
		return "BIGINT"
	case "float", "double":
		return "DOUBLE"
	case "decimal":
		return "DECIMAL"
	case "date":
		return "DATE"
	case "timestamp":
		return "TIMESTAMP"
	case "string":
		return "VARCHAR"
	case "binary":
		return "VARBINARY"
	case "array":
		return "ARRAY"
	case "map":
		return "MAP"
	case "struct":
		return "STRUCT"
	default:
		return deltaType
	}
}

// ParseDeltaTablePath parses a Delta table path
func ParseDeltaTablePath(path string) (catalog, schema, table string, err error) {
	// Delta paths can be:
	// - catalog.schema.table
	// - s3://bucket/path/to/table
	// - /local/path/to/table

	// Simple implementation for three-part name
	parts := bytes.Split([]byte(path), []byte{'.'})
	if len(parts) == 3 {
		return string(parts[0]), string(parts[1]), string(parts[2]), nil
	}

	return "", "", "", fmt.Errorf("invalid Delta table path: %s", path)
}
