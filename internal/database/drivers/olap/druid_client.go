package olap

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// DruidRESTClient handles Druid SQL API requests
type DruidRESTClient struct {
	baseURL    string
	httpClient *http.Client
	brokerURL  string
}

// NewDruidRESTClient creates a new Druid REST client
func NewDruidRESTClient(brokerURL string) *DruidRESTClient {
	return &DruidRESTClient{
		baseURL:   brokerURL,
		brokerURL: brokerURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// ExecuteSQLQuery executes a SQL query via Druid SQL API
func (c *DruidRESTClient) ExecuteSQLQuery(ctx context.Context, sql string) (*DruidQueryResult, error) {
	url := fmt.Sprintf("%s/druid/v2/sql", c.baseURL)

	request := DruidSQLRequest{
		Query: sql,
	}

	reqBody, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(reqBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("query failed with status %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var results []map[string]interface{}
	if err := json.Unmarshal(body, &results); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return c.parseResults(results)
}

// DruidSQLRequest represents Druid SQL request
type DruidSQLRequest struct {
	Query          string `json:"query"`
	ResultFormat   string `json:"resultFormat,omitempty"`
	Header         bool   `json:"header,omitempty"`
	Parameters     []DruidQueryParameter `json:"parameters,omitempty"`
	Context        map[string]interface{} `json:"context,omitempty"`
}

// DruidQueryParameter represents query parameter
type DruidQueryParameter struct {
	Type  string      `json:"type"`
	Value interface{} `json:"value"`
}

// DruidQueryResult represents query result
type DruidQueryResult struct {
	Rows       []map[string]interface{}
	Columns    []string
	RowCount   int
	QueryID    string
	ElapsedMS  int64
}

// parseResults parses Druid query results
func (c *DruidRESTClient) parseResults(results []map[string]interface{}) (*DruidQueryResult, error) {
	if len(results) == 0 {
		return &DruidQueryResult{
			Rows:     []map[string]interface{}{},
			Columns:  []string{},
			RowCount: 0,
		}, nil
	}

	// Extract columns from first row
	columns := make([]string, 0, len(results[0]))
	for key := range results[0] {
		columns = append(columns, key)
	}

	return &DruidQueryResult{
		Rows:     results,
		Columns:  columns,
		RowCount: len(results),
	}, nil
}

// GetDatasources retrieves list of datasources
func (c *DruidRESTClient) GetDatasources(ctx context.Context) ([]string, error) {
	url := fmt.Sprintf("%s/druid/coordinator/v1/datasources", c.baseURL)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var datasources []string
	if err := json.Unmarshal(body, &datasources); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return datasources, nil
}

// GetDatasourceMetadata retrieves metadata for a datasource
func (c *DruidRESTClient) GetDatasourceMetadata(ctx context.Context, datasource string) (*DruidDatasourceMetadata, error) {
	url := fmt.Sprintf("%s/druid/coordinator/v1/datasources/%s", c.baseURL, datasource)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var metadata DruidDatasourceMetadata
	if err := json.Unmarshal(body, &metadata); nil != err {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &metadata, nil
}

// DruidDatasourceMetadata represents datasource metadata
type DruidDatasourceMetadata struct {
	Name       string    `json:"name"`
	Segments   int       `json:"segments"`
	Columns    []string  `json:"columns"`
	Properties map[string]interface{} `json:"properties"`
}

// GetClusterStatus retrieves cluster status
func (c *DruidRESTClient) GetClusterStatus(ctx context.Context) (*DruidClusterStatus, error) {
	url := fmt.Sprintf("%s/status", c.baseURL)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var status DruidClusterStatus
	if err := json.Unmarshal(body, &status); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return &status, nil
}

// DruidClusterStatus represents cluster status
type DruidClusterStatus struct {
	Version    string `json:"version"`
	Coordinator bool  `json:"coordinator"`
	Overlord   bool   `json:"overlord"`
	Broker     bool   `json:"broker"`
}

// CancelQuery cancels a running query
func (c *DruidRESTClient) CancelQuery(ctx context.Context, queryID string) error {
	url := fmt.Sprintf("%s/druid/v2/sql/%s", c.baseURL, queryID)

	req, err := http.NewRequestWithContext(ctx, "DELETE", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to cancel query with status %d", resp.StatusCode)
	}

	return nil
}
