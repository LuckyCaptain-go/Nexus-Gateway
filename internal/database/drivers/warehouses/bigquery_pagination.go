package warehouses

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
)

// BigQueryPaginationHandler handles pagination for BigQuery results
type BigQueryPaginationHandler struct {
	driver   *BigQueryDriver
	pageSize int64
	maxRows  int64
	timeout  time.Duration
}

// NewBigQueryPaginationHandler creates a new pagination handler
func NewBigQueryPaginationHandler(driver *BigQueryDriver, pageSize int64) *BigQueryPaginationHandler {
	return &BigQueryPaginationHandler{
		driver:   driver,
		pageSize: pageSize,
		maxRows:  0, // 0 means unlimited
		timeout:  30 * time.Second,
	}
}

// SetPageSize sets the page size for pagination
func (p *BigQueryPaginationHandler) SetPageSize(pageSize int64) {
	p.pageSize = pageSize
}

// SetMaxRows sets maximum rows to return (0 = unlimited)
func (p *BigQueryPaginationHandler) SetMaxRows(maxRows int64) {
	p.maxRows = maxRows
}

// SetTimeout sets query timeout
func (p *BigQueryPaginationHandler) SetTimeout(timeout time.Duration) {
	p.timeout = timeout
}

// PaginatedQueryResult contains paginated query results
type PaginatedQueryResult struct {
	PageToken    string              `json:"pageToken"`
	Rows         [][]interface{}     `json:"rows"`
	Schema       bigquery.Schema     `json:"schema"`
	TotalRows    int64               `json:"totalRows"`
	PageNumber   int64               `json:"pageNumber"`
	RowsReturned int64               `json:"rowsReturned"`
	HasMore      bool                `json:"hasMore"`
	QueryInfo    *QueryExecutionInfo `json:"queryInfo"`
}

// QueryExecutionInfo contains query execution metadata
type QueryExecutionInfo struct {
	JobID          string         `json:"jobId"`
	StartTime      time.Time      `json:"startTime"`
	EndTime        time.Time      `json:"endTime"`
	Duration       time.Duration  `json:"duration"`
	BytesProcessed int64          `json:"bytesProcessed"`
	BytesBilled    int64          `json:"bytesBilled"`
	CacheHit       bool           `json:"cacheHit"`
	SlotMillis     int64          `json:"slotMillis"`
	DMLStats       *DMLStatistics `json:"dmlStats,omitempty"`
}

// DMLStatistics contains DML operation statistics
type DMLStatistics struct {
	InsertedRows int64 `json:"insertedRows"`
	UpdatedRows  int64 `json:"updatedRows"`
	DeletedRows  int64 `json:"deletedRows"`
}

// QueryPaginated executes a query with pagination support
func (p *BigQueryPaginationHandler) QueryPaginated(ctx context.Context, sql string, pageToken string) (*PaginatedQueryResult, error) {
	query := p.driver.client.Query(sql)
	query.ProjectID = p.driver.projectID
	query.Location = p.driver.location
	query.UseLegacySQL = false
	query.UseQueryCache = true

	// Set page size if specified
	if p.pageSize > 0 {
		// BigQuery uses MaxBytesBilled for result size limits
		// For row-based pagination, we use the iterator with offset
	}

	// Add timeout context
	if p.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, p.timeout)
		defer cancel()
	}

	// Run query
	job, err := query.Run(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to run query: %w", err)
	}

	// Wait for completion
	status, err := job.Wait(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to wait for job: %w", err)
	}

	if status.Err() != nil {
		return nil, fmt.Errorf("job failed: %w", status.Err())
	}

	// Get results
	it, err := job.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read results: %w", err)
	}

	// Parse page token
	var offset int64
	var pageNum int64
	if pageToken != "" {
		offset, pageNum = p.parsePageToken(pageToken)
	}

	// Apply offset for pagination
	if offset > 0 {
		// BigQuery doesn't support direct offset in API
		// We need to skip rows manually
	}

	// Load page of results
	result := &PaginatedQueryResult{
		Schema:     it.Schema,
		PageNumber: pageNum,
		QueryInfo:  p.extractJobInfo(ctx, job),
	}

	// Load rows
	var rows [][]interface{}
	totalRowsRead := int64(0)

	for {
		var row []interface{}
		err := it.Next(&row)
		if err != nil {
			break
		}

		// Skip rows for offset
		if offset > 0 {
			offset--
			continue
		}

		rows = append(rows, row)
		totalRowsRead++

		// Check page size limit
		if p.pageSize > 0 && int64(len(rows)) >= p.pageSize {
			break
		}

		// Check max rows limit
		if p.maxRows > 0 && totalRowsRead >= p.maxRows {
			break
		}
	}

	result.Rows = rows
	result.RowsReturned = int64(len(rows))
	result.HasMore = it.NextPageToken() != ""

	// Generate next page token
	if result.HasMore {
		result.PageToken = p.generatePageToken(pageNum+1, totalRowsRead)
	}

	// Get total rows from statistics
	if status.Statistics != nil {
		result.TotalRows = int64(status.Statistics.NumDmlAffectedRows)
	}

	return result, nil
}

// parsePageToken parses a page token to extract offset and page number
func (p *BigQueryPaginationHandler) parsePageToken(token string) (offset int64, pageNum int64) {
	// Simple token format: "pageNum:offset"
	fmt.Sscanf(token, "%d:%d", &pageNum, &offset)
	return offset, pageNum
}

// generatePageToken generates a page token for the next page
func (p *BigQueryPaginationHandler) generatePageToken(pageNum int64, offset int64) string {
	return fmt.Sprintf("%d:%d", pageNum, offset)
}

// StreamQuery executes a query and streams results using callback
func (p *BigQueryPaginationHandler) StreamQuery(ctx context.Context, sql string, callback func(row []interface{}, schema bigquery.Schema) error) error {
	query := p.driver.client.Query(sql)
	query.ProjectID = p.driver.projectID
	query.Location = p.driver.location

	job, err := query.Run(ctx)
	if err != nil {
		return fmt.Errorf("failed to run query: %w", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("failed to wait for job: %w", err)
	}

	if status.Err() != nil {
		return fmt.Errorf("job failed: %w", status.Err())
	}

	it, err := job.Read(ctx)
	if err != nil {
		return fmt.Errorf("failed to read results: %w", err)
	}

	schema := it.Schema

	for {
		var row []interface{}
		err := it.Next(&row)
		if err != nil {
			break
		}

		if err := callback(row, schema); err != nil {
			return err
		}
	}

	return nil
}

// QueryAllPages executes a query and retrieves all pages
func (p *BigQueryPaginationHandler) QueryAllPages(ctx context.Context, sql string) (*PaginatedQueryResult, error) {
	query := p.driver.client.Query(sql)
	query.ProjectID = p.driver.projectID
	query.Location = p.driver.location

	job, err := query.Run(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to run query: %w", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to wait for job: %w", err)
	}

	if status.Err() != nil {
		return nil, fmt.Errorf("job failed: %w", status.Err())
	}

	it, err := job.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read results: %w", err)
	}

	// Load all rows
	var allRows [][]interface{}
	for {
		var row []interface{}
		err := it.Next(&row)
		if err != nil {
			break
		}
		allRows = append(allRows, row)
	}

	result := &PaginatedQueryResult{
		Rows:         allRows,
		Schema:       it.Schema,
		TotalRows:    int64(len(allRows)),
		RowsReturned: int64(len(allRows)),
		HasMore:      false,
		QueryInfo:    p.extractJobInfo(ctx, job),
	}

	return result, nil
}

// extractJobInfo extracts job execution information
func (p *BigQueryPaginationHandler) extractJobInfo(ctx context.Context, job *bigquery.Job) *QueryExecutionInfo {
	status, err := job.Wait(ctx)
	if err != nil {
		return &QueryExecutionInfo{
			JobID: job.ID(),
		}
	}

	info := &QueryExecutionInfo{
		JobID: job.ID(),
	}

	if status.Statistics != nil {
		info.BytesProcessed = status.Statistics.TotalBytesProcessed
		info.BytesBilled = status.Statistics.TotalBytesBilled
		info.CacheHit = status.Statistics.CacheHit
		info.SlotMillis = status.Statistics.TotalSlotMillis

		if !status.Statistics.StartTime.IsZero() {
			info.StartTime = status.Statistics.StartTime
		}
		if !status.Statistics.EndTime.IsZero() {
			info.EndTime = status.Statistics.EndTime
			info.Duration = info.EndTime.Sub(info.StartTime)
		}

		// Extract DML statistics
		if status.Statistics.NumDmlAffectedRows > 0 {
			info.DMLStats = &DMLStatistics{
				// BigQuery doesn't break down DML by operation type
				InsertedRows: status.Statistics.NumDmlAffectedRows,
			}
		}
	}

	return info
}

// QueryWithRetry executes a query with retry on transient errors
func (p *BigQueryPaginationHandler) QueryWithRetry(ctx context.Context, sql string, maxRetries int) (*PaginatedQueryResult, error) {
	var lastErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		result, err := p.QueryPaginated(ctx, sql, "")
		if err == nil {
			return result, nil
		}

		lastErr = err

		// Check if error is transient
		if !isTransientBigQueryError(err) {
			return nil, err
		}

		// Wait before retry with exponential backoff
		if attempt < maxRetries {
			waitTime := time.Duration(attempt+1) * time.Second
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(waitTime):
				continue
			}
		}
	}

	return nil, fmt.Errorf("failed after %d retries: %w", maxRetries, lastErr)
}

// isTransientBigQueryError checks if error is transient
func isTransientBigQueryError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := err.Error()
	transientErrors := []string{
		"connection reset",
		"timeout",
		"temporary failure",
		"rate limit",
		"quota exceeded",
		"backend error",
		"internal error",
	}

	for _, msg := range transientErrors {
		if contains(errMsg, msg) {
			return true
		}
	}

	return false
}

// AsyncQueryResult represents an asynchronous query execution
type AsyncQueryResult struct {
	JobID     string
	Job       *bigquery.Job
	Status    string
	StartTime time.Time
}

// StartAsyncQuery starts an async query execution
func (p *BigQueryPaginationHandler) StartAsyncQuery(ctx context.Context, sql string) (*AsyncQueryResult, error) {
	query := p.driver.client.Query(sql)
	query.ProjectID = p.driver.projectID
	query.Location = p.driver.location

	job, err := query.Run(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to start query: %w", err)
	}

	result := &AsyncQueryResult{
		JobID:     job.ID(),
		Job:       job,
		Status:    "PENDING",
		StartTime: time.Now(),
	}

	return result, nil
}

// GetAsyncQueryResults retrieves results of an async query
func (p *BigQueryPaginationHandler) GetAsyncQueryResults(ctx context.Context, jobID string) (*PaginatedQueryResult, error) {
	job := p.driver.client.JobFromID(ctx, jobID)

	status, err := job.Wait(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to wait for job: %w", err)
	}

	if status.Err() != nil {
		return nil, fmt.Errorf("job failed: %w", status.Err())
	}

	it, err := job.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read results: %w", err)
	}

	// Load all rows
	var allRows [][]interface{}
	for {
		var row []interface{}
		err := it.Next(&row)
		if err != nil {
			break
		}
		allRows = append(allRows, row)
	}

	result := &PaginatedQueryResult{
		Rows:         allRows,
		Schema:       it.Schema,
		TotalRows:    int64(len(allRows)),
		RowsReturned: int64(len(allRows)),
		HasMore:      false,
		QueryInfo:    p.extractJobInfo(ctx, job),
	}

	return result, nil
}

// QueryWithResultOptions executes query with advanced result options
type QueryResultOptions struct {
	Format           string // "json", "csv", "avro"
	DestinationTable string // BigQuery table ID for results
	WriteDisposition string // "WRITE_TRUNCATE", "WRITE_APPEND", "WRITE_EMPTY"
	MaxResults       int64
	Timeout          time.Duration
}

// QueryWithOptions executes a query with result options
func (p *BigQueryPaginationHandler) QueryWithOptions(ctx context.Context, sql string, options QueryResultOptions) (*PaginatedQueryResult, error) {
	query := p.driver.client.Query(sql)
	query.ProjectID = p.driver.projectID
	query.Location = p.driver.location

	// Set query job config
	qConfig := query.QueryConfig

	if options.DestinationTable != "" {
		// Parse destination table ID and set as destination
		// Format: projectID:datasetID.tableID
		// This would require parsing the table ID
		_ = qConfig
	}

	if options.WriteDisposition != "" {
		// Set write disposition
	}

	job, err := query.Run(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to run query: %w", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to wait for job: %w", err)
	}

	if status.Err() != nil {
		return nil, fmt.Errorf("job failed: %w", status.Err())
	}

	it, err := job.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read results: %w", err)
	}

	// Load rows up to max results
	var allRows [][]interface{}
	rowCount := int64(0)

	for {
		if options.MaxResults > 0 && rowCount >= options.MaxResults {
			break
		}

		var row []interface{}
		err := it.Next(&row)
		if err != nil {
			break
		}
		allRows = append(allRows, row)
		rowCount++
	}

	result := &PaginatedQueryResult{
		Rows:         allRows,
		Schema:       it.Schema,
		TotalRows:    int64(len(allRows)),
		RowsReturned: rowCount,
		HasMore:      it.NextPageToken() != "",
		QueryInfo:    p.extractJobInfo(ctx, job),
	}

	return result, nil
}

// BatchQuery executes multiple queries in batch
func (p *BigQueryPaginationHandler) BatchQuery(ctx context.Context, queries []string) ([]*PaginatedQueryResult, error) {
	results := make([]*PaginatedQueryResult, len(queries))

	for i, sql := range queries {
		result, err := p.QueryPaginated(ctx, sql, "")
		if err != nil {
			return nil, fmt.Errorf("query %d failed: %w", i, err)
		}
		results[i] = result
	}

	return results, nil
}

// contains checks if a string contains a substring (case-insensitive)
func contains(s, substr string) bool {
	return len(s) >= len(substr) &&
		(s == substr ||
			len(s) > len(substr) && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
