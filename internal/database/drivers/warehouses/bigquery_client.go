package warehouses

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"nexus-gateway/internal/model"
)

// BigQueryRESTClient provides REST API wrapper for BigQuery
// This serves as an alternative to the native Go client for scenarios
// where REST API is preferred or for compatibility layers
type BigQueryRESTClient struct {
	baseURL    string // BigQuery API endpoint
	httpClient *http.Client
	accessToken string // OAuth2 access token
	projectID  string
	location   string
}

// BigQueryRESTConfig holds REST client configuration
type BigQueryRESTConfig struct {
	Endpoint   string // BigQuery API endpoint (default: https://bigquery.googleapis.com)
	AccessToken string // OAuth2 access token
	ProjectID  string
	Location   string
	Timeout    time.Duration
}

// NewBigQueryRESTClient creates a new BigQuery REST client
func NewBigQueryRESTClient(config *BigQueryRESTConfig) (*BigQueryRESTClient, error) {
	if config.ProjectID == "" {
		return nil, fmt.Errorf("project ID is required")
	}
	if config.AccessToken == "" {
		return nil, fmt.Errorf("access token is required")
	}

	endpoint := config.Endpoint
	if endpoint == "" {
		endpoint = "https://bigquery.googleapis.com"
	}

	timeout := config.Timeout
	if timeout == 0 {
		timeout = 120 * time.Second
	}

	return &BigQueryRESTClient{
		baseURL:    endpoint,
		httpClient: &http.Client{Timeout: timeout},
		accessToken: config.AccessToken,
		projectID:  config.ProjectID,
		location:   config.Location,
	}, nil
}

// ==============================================================================
// Query Execution
// ==============================================================================

// QueryRequest represents a BigQuery query request
type QueryRequest struct {
	Query              string                 `json:"query"`
	Kind               string                 `json:"kind,omitempty"`
	TimeoutMs          int64                  `json:"timeoutMs,omitempty"`
	UseLegacySQL       bool                   `json:"useLegacySql,omitempty"`
	UseQueryCache      bool                   `json:"useQueryCache,omitempty"`
	DefaultDataset     *DatasetReference      `json:"defaultDataset,omitempty"`
	MaxResults         int64                  `json:"maxResults,omitempty"`
	ParameterMode      string                 `json:"parameterMode,omitempty"`
	QueryParameters    []QueryParameter       `json:"queryParameters,omitempty"`
	DryRun             bool                   `json:"dryRun,omitempty"`
	PreserveNulls      bool                   `json:"preserveNulls,omitempty"`
	Labels             map[string]string      `json:"labels,omitempty"`
	RequestID          string                 `json:"requestId,omitempty"`
	ConnectionProperties []ConnectionProperty  `json:"connectionProperties,omitempty"`
}

// DatasetReference represents a dataset reference
type DatasetReference struct {
	ProjectID string `json:"projectId"`
	DatasetID string `json:"datasetId"`
}

// QueryParameter represents a query parameter
type QueryParameter struct {
	Name         string      `json:"name"`
	ParameterValue interface{} `json:"parameterValue"`
	ParameterType *QueryParameterType `json:"parameterType"`
}

// QueryParameterType represents parameter type
type QueryParameterType struct {
	Type    string `json:"type"`
	ArrayType *QueryParameterType `json:"arrayType,omitempty"`
	StructTypes []StructType `json:"structTypes,omitempty"`
}

// StructType represents struct field type
type StructType struct {
	Name string `json:"name"`
	Type *QueryParameterType `json:"type"`
}

// ConnectionProperty represents connection properties
type ConnectionProperty struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// QueryResponse represents a BigQuery query response
type QueryResponse struct {
	Kind              string          `json:"kind"`
	Schema            *TableSchema    `json:"schema,omitempty"`
	JobReference      *JobReference   `json:"jobReference"`
	TotalRows         int64           `json:"totalRows,omitempty"`
	PageToken         string          `json:"pageToken,omitempty"`
	Rows              []TableRow      `json:"rows,omitempty"`
	Errors            []ErrorProto    `json:"errors,omitempty"`
	JobComplete       bool            `json:"jobComplete"`
	CacheHit          bool            `json:"cacheHit,omitempty"`
	DMLStats          *DmlStatistics  `json:"dmlStats,omitempty"`
}

// TableSchema represents table schema
type TableSchema struct {
	Fields []TableFieldSchema `json:"fields"`
}

// TableFieldSchema represents field schema
type TableFieldSchema struct {
	Name       string            `json:"name"`
	Type       string            `json:"type"`
	Mode       string            `json:"mode,omitempty"`
	Fields     []TableFieldSchema `json:"fields,omitempty"`
}

// TableRow represents a table row
type TableRow struct {
	F []TableCell `json:"f"`
}

// TableCell represents a table cell
type TableCell struct {
	V interface{} `json:"v"`
}

// JobReference represents a job reference
type JobReference struct {
	ProjectID string `json:"projectId"`
	JobID     string `json:"jobId"`
	Location  string `json:"location,omitempty"`
}

// ErrorProto represents an error
type ErrorProto struct {
	Reason  string `json:"reason"`
	Location string `json:"location,omitempty"`
	Message string `json:"message"`
}

// DmlStatistics represents DML statistics
type DmlStatistics struct {
	InsertedRowCount int64 `json:"insertedRowCount"`
	UpdatedRowCount  int64 `json:"updatedRowCount"`
	DeletedRowCount  int64 `json:"deletedRowCount"`
}

// Query executes a synchronous query
func (c *BigQueryRESTClient) Query(ctx context.Context, req *QueryRequest) (*QueryResponse, error) {
	apiURL := fmt.Sprintf("%s/bigquery/v2/projects/%s/queries", c.baseURL, c.projectID)

	// Set default values
	req.Kind = "bigquery#queryRequest"
	if req.UseQueryCache && req.Query == "" {
		req.UseQueryCache = true
	}

	body, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", apiURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	c.setAuthHeader(httpReq)
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("query failed with status %d: %s", resp.StatusCode, string(body))
	}

	var queryResp QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&queryResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &queryResp, nil
}

// QueryWithResults executes a query and returns results
func (c *BigQueryRESTClient) QueryWithResults(ctx context.Context, sql string, maxResults int64) (*QueryResponse, error) {
	req := &QueryRequest{
		Query:         sql,
		UseLegacySQL:  false,
		UseQueryCache: true,
		MaxResults:    maxResults,
	}

	if c.location != "" {
		req.Labels = map[string]string{"location": c.location}
	}

	return c.Query(ctx, req)
}

// GetQueryResults retrieves results of a query job
func (c *BigQueryRESTClient) GetQueryResults(ctx context.Context, jobID string, pageToken string, maxResults int64) (*QueryResponse, error) {
	apiURL := fmt.Sprintf("%s/bigquery/v2/projects/%s/queries/%s", c.baseURL, c.projectID, jobID)

	if pageToken != "" || maxResults > 0 {
		params := url.Values{}
		if pageToken != "" {
			params.Add("pageToken", pageToken)
		}
		if maxResults > 0 {
			params.Add("maxResults", fmt.Sprintf("%d", maxResults))
		}
		apiURL += "?" + params.Encode()
	}

	httpReq, err := http.NewRequestWithContext(ctx, "GET", apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	c.setAuthHeader(httpReq)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("get query results failed with status %d: %s", resp.StatusCode, string(body))
	}

	var queryResp QueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&queryResp); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &queryResp, nil
}

// ==============================================================================
// Jobs API
// ==============================================================================

// Job represents a BigQuery job
type Job struct {
	JobReference  *JobReference `json:"jobReference"`
	Configuration *JobConfiguration `json:"configuration"`
	Status        *JobStatus `json:"status"`
	Statistics    *JobStatistics `json:"statistics,omitempty"`
}

// JobConfiguration represents job configuration
type JobConfiguration struct {
	Query    *QueryJobConfig    `json:"query,omitempty"`
	Load     *LoadJobConfig     `json:"load,omitempty"`
	Extract  *ExtractJobConfig  `json:"extract,omitempty"`
	TableCopy *TableCopyJobConfig `json:"tableCopy,omitempty"`
}

// QueryJobConfig represents query job configuration
type QueryJobConfig struct {
	Query              string `json:"query"`
	DestinationTable   *TableReference `json:"destinationTable,omitempty"`
	CreateDisposition  string `json:"createDisposition,omitempty"`
	WriteDisposition   string `json:"writeDisposition,omitempty"`
	DefaultDataset     *DatasetReference `json:"defaultDataset,omitempty"`
	Priority           string `json:"priority,omitempty"`
	UseLegacySQL       bool   `json:"useLegacySql,omitempty"`
	UseQueryCache      bool   `json:"useQueryCache,omitempty"`
}

// LoadJobConfig represents load job configuration
type LoadJobConfig struct {
	DestinationTable *TableReference `json:"destinationTable"`
	SourceURIs       []string        `json:"sourceUris"`
	Schema           *TableSchema    `json:"schema,omitempty"`
	SourceFormat     string          `json:"sourceFormat,omitempty"`
	WriteDisposition string         `json:"writeDisposition,omitempty"`
	CreateDisposition string        `json:"createDisposition,omitempty"`
	SkipLeadingRows  int64           `json:"skipLeadingRows,omitempty"`
	FieldDelimiter   string         `json:"fieldDelimiter,omitempty"`
	Quote            string         `json:"quote,omitempty"`
	AllowQuotedNewlines bool        `json:"allowQuotedNewlines,omitempty"`
}

// ExtractJobConfig represents extract job configuration
type ExtractJobConfig struct {
	SourceTable      *TableReference `json:"sourceTable"`
	DestinationURIs  []string        `json:"destinationUris"`
	DestinationFormat string         `json:"destinationFormat,omitempty"`
	Compression      string         `json:"compression,omitempty"`
	FieldDelimiter   string         `json:"fieldDelimiter,omitempty"`
}

// TableCopyJobConfig represents table copy job configuration
type TableCopyJobConfig struct {
	SourceTables      []*TableReference `json:"sourceTables"`
	DestinationTable  *TableReference `json:"destinationTable"`
	WriteDisposition  string          `json:"writeDisposition,omitempty"`
	CreateDisposition string          `json:"createDisposition,omitempty"`
}

// TableReference represents a table reference
type TableReference struct {
	ProjectID string `json:"projectId"`
	DatasetID string `json:"datasetId"`
	TableID   string `json:"tableId"`
}

// JobStatus represents job status
type JobStatus struct {
	State       string      `json:"state"`
	ErrorResult *ErrorProto `json:"errorResult,omitempty"`
	Errors      []ErrorProto `json:"errors,omitempty"`
}

// JobStatistics represents job statistics
type JobStatistics struct {
	Query        *QueryStatistics `json:"query,omitempty"`
	Load         *LoadStatistics `json:"load,omitempty"`
	Extract      *ExtractStatistics `json:"extract,omitempty"`
	StartTime    string `json:"startTime,omitempty"`
	EndTime      string `json:"endTime,omitempty"`
	TotalDuration string `json:"totalDuration,omitempty"`
}

// QueryStatistics represents query job statistics
type QueryStatistics struct {
	TotalBytesProcessed     string `json:"totalBytesProcessed"`
	TotalBytesBilled        string `json:"totalBytesBilled"`
	CacheHit                bool   `json:"cacheHit"`
	NumDmlAffectedRows      int64  `json:"numDmlAffectedRows,omitempty"`
	DmlStats                *DmlStatistics `json:"dmlStats,omitempty"`
}

// LoadStatistics represents load job statistics
type LoadStatistics struct {
	InputFileBytes    int64 `json:"inputFileBytes"`
	InputFileCount    int64 `json:"inputFileCount"`
	OutputBytes       int64 `json:"outputBytes"`
	OutputRowCount    int64 `json:"outputRowCount"`
	BadRecords        int64 `json:"badRecords,omitempty"`
}

// ExtractStatistics represents extract job statistics
type ExtractStatistics struct {
	InputFileBytes    int64 `json:"inputFileBytes"`
	RowsExtracted     int64 `json:"rowsExtracted,omitempty"`
}

// StartJob starts a new job
func (c *BigQueryRESTClient) StartJob(ctx context.Context, job *Job) (*Job, error) {
	apiURL := fmt.Sprintf("%s/bigquery/v2/projects/%s/jobs", c.baseURL, c.projectID)

	if c.location != "" {
		apiURL += fmt.Sprintf("?location=%s", c.location)
	}

	body, err := json.Marshal(job)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", apiURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	c.setAuthHeader(httpReq)
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("start job failed with status %d: %s", resp.StatusCode, string(body))
	}

	var startedJob Job
	if err := json.NewDecoder(resp.Body).Decode(&startedJob); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &startedJob, nil
}

// StartQueryJob starts a query job
func (c *BigQueryRESTClient) StartQueryJob(ctx context.Context, sql string, config *QueryJobConfig) (*Job, error) {
	if config == nil {
		config = &QueryJobConfig{}
	}
	config.Query = sql

	job := &Job{
		Configuration: &JobConfiguration{
			Query: config,
		},
	}

	return c.StartJob(ctx, job)
}

// GetJob retrieves job information
func (c *BigQueryRESTClient) GetJob(ctx context.Context, jobID string) (*Job, error) {
	apiURL := fmt.Sprintf("%s/bigquery/v2/projects/%s/jobs/%s", c.baseURL, c.projectID, jobID)

	httpReq, err := http.NewRequestWithContext(ctx, "GET", apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	c.setAuthHeader(httpReq)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("get job failed with status %d: %s", resp.StatusCode, string(body))
	}

	var job Job
	if err := json.NewDecoder(resp.Body).Decode(&job); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &job, nil
}

// CancelJob cancels a running job
func (c *BigQueryRESTClient) CancelJob(ctx context.Context, jobID string) error {
	apiURL := fmt.Sprintf("%s/bigquery/v2/projects/%s/jobs/%s/cancel", c.baseURL, c.projectID, jobID)

	httpReq, err := http.NewRequestWithContext(ctx, "POST", apiURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	c.setAuthHeader(httpReq)
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("cancel job failed with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// ==============================================================================
// Tables API
// ==============================================================================

// GetTable retrieves table metadata
func (c *BigQueryRESTClient) GetTable(ctx context.Context, datasetID, tableID string) (*Table, error) {
	apiURL := fmt.Sprintf("%s/bigquery/v2/projects/%s/datasets/%s/tables/%s",
		c.baseURL, c.projectID, datasetID, tableID)

	httpReq, err := http.NewRequestWithContext(ctx, "GET", apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	c.setAuthHeader(httpReq)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("get table failed with status %d: %s", resp.StatusCode, string(body))
	}

	var table Table
	if err := json.NewDecoder(resp.Body).Decode(&table); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &table, nil
}

// ListTables lists tables in a dataset
func (c *BigQueryRESTClient) ListTables(ctx context.Context, datasetID string) ([]*Table, error) {
	apiURL := fmt.Sprintf("%s/bigquery/v2/projects/%s/datasets/%s/tables",
		c.baseURL, c.projectID, datasetID)

	httpReq, err := http.NewRequestWithContext(ctx, "GET", apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	c.setAuthHeader(httpReq)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("list tables failed with status %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Tables  []*Table `json:"tables"`
		NextPageToken string `json:"nextPageToken,omitempty"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return result.Tables, nil
}

// Table represents a BigQuery table
type Table struct {
	TableReference *TableReference `json:"tableReference"`
	Schema         *TableSchema `json:"schema,omitempty"`
	Type           string `json:"type,omitempty"`
}

// ==============================================================================
// Datasets API
// ==============================================================================

// GetDataset retrieves dataset information
func (c *BigQueryRESTClient) GetDataset(ctx context.Context, datasetID string) (*Dataset, error) {
	apiURL := fmt.Sprintf("%s/bigquery/v2/projects/%s/datasets/%s",
		c.baseURL, c.projectID, datasetID)

	httpReq, err := http.NewRequestWithContext(ctx, "GET", apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	c.setAuthHeader(httpReq)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("get dataset failed with status %d: %s", resp.StatusCode, string(body))
	}

	var dataset Dataset
	if err := json.NewDecoder(resp.Body).Decode(&dataset); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &dataset, nil
}

// ListDatasets lists datasets in the project
func (c *BigQueryRESTClient) ListDatasets(ctx context.Context) ([]*Dataset, error) {
	apiURL := fmt.Sprintf("%s/bigquery/v2/projects/%s/datasets", c.baseURL, c.projectID)

	httpReq, err := http.NewRequestWithContext(ctx, "GET", apiURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	c.setAuthHeader(httpReq)

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("list datasets failed with status %d: %s", resp.StatusCode, string(body))
	}

	var result struct {
		Datasets []*Dataset `json:"datasets"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return result.Datasets, nil
}

// Dataset represents a BigQuery dataset
type Dataset struct {
	DatasetReference *DatasetReference `json:"datasetReference"`
	FriendlyName     string `json:"friendlyName,omitempty"`
}

// ==============================================================================
// Helper Methods
// ==============================================================================

// setAuthHeader sets the Bearer token authorization header
func (c *BigQueryRESTClient) setAuthHeader(req *http.Request) {
	req.Header.Set("Authorization", "Bearer "+c.accessToken)
}

// ToStandardSchema converts BigQuery schema to standard schema
func (c *BigQueryRESTClient) ToStandardSchema(bqSchema *TableSchema) model.TableSchema {
	stdSchema := model.TableSchema{
		Columns: make([]model.ColumnInfo, 0, len(bqSchema.Fields)),
	}

	for _, field := range bqSchema.Fields {
		col := model.ColumnInfo{
			Name:     field.Name,
			Type:     mapBigQueryTypeToStandard(field.Type),
			Nullable: field.Mode != "REQUIRED",
		}

		if field.Type == "RECORD" && len(field.Fields) > 0 {
			col.NestedFields = c.convertNestedFields(field.Fields)
		}

		stdSchema.Columns = append(stdSchema.Columns, col)
	}

	return stdSchema
}

// mapBigQueryTypeToStandard maps BigQuery type to standard type
func mapBigQueryTypeToStandard(bqType string) model.StandardizedType {
	switch bqType {
	case "STRING":
		return model.StandardizedTypeString
	case "BYTES":
		return model.StandardizedTypeBinary
	case "INTEGER":
		return model.StandardizedTypeInt64
	case "FLOAT":
		return model.StandardizedTypeFloat64
	case "BOOLEAN":
		return model.StandardizedTypeBoolean
	case "TIMESTAMP":
		return model.StandardizedTypeTimestamp
	case "DATE":
		return model.StandardizedTypeDate
	case "TIME":
		return model.StandardizedTypeTime
	case "DATETIME":
		return model.StandardizedTypeDateTime
	case "NUMERIC", "BIGNUMERIC":
		return model.StandardizedTypeDecimal
	case "RECORD", "STRUCT":
		return model.StandardizedTypeStruct
	case "GEOGRAPHY":
		return model.StandardizedTypeGeography
	default:
		return model.StandardizedTypeString
	}
}

// convertNestedFields converts nested fields for RECORD types
func (c *BigQueryRESTClient) convertNestedFields(fields []TableFieldSchema) []model.ColumnInfo {
	cols := make([]model.ColumnInfo, 0, len(fields))

	for _, field := range fields {
		col := model.ColumnInfo{
			Name:     field.Name,
			Type:     mapBigQueryTypeToStandard(field.Type),
			Nullable: field.Mode != "REQUIRED",
		}

		if field.Type == "RECORD" && len(field.Fields) > 0 {
			col.NestedFields = c.convertNestedFields(field.Fields)
		}

		cols = append(cols, col)
	}

	return cols
}

// UpdateAccessToken updates the access token
func (c *BigQueryRESTClient) UpdateAccessToken(token string) {
	c.accessToken = token
}
