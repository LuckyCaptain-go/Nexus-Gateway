package service

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"nexus-gateway/internal/database"
	"nexus-gateway/internal/model"
	"nexus-gateway/internal/repository"
	"nexus-gateway/internal/security"
	"nexus-gateway/internal/utils"
)

type QueryService interface {
	ExecuteQuery(ctx context.Context, req *model.QueryRequest) (*model.QueryResponse, error)
	ValidateQuery(ctx context.Context, req *model.QueryRequest) error
	GetQueryStats(ctx context.Context) (*model.QueryStats, error)
}

type queryService struct {
	datasourceRepo repository.DataSourceRepository
	connPool       *database.ConnectionPool
	sqlValidator   *security.SQLValidator
	typeMapper     *utils.DataTypeMapper
	stats          *queryStats
}

type queryStats struct {
	totalQueries      int64
	successfulQueries int64
	failedQueries     int64
	totalExecutionTime int64 // in nanoseconds
	lastQueryTime     time.Time
	queriesByType     map[string]int64
	mutex             sync.RWMutex
}

// NewQueryService creates a new instance of QueryService
func NewQueryService(datasourceRepo repository.DataSourceRepository, connPool *database.ConnectionPool) QueryService {
	return &queryService{
		datasourceRepo: datasourceRepo,
		connPool:       connPool,
		sqlValidator:   security.NewSQLValidator(10000),
		typeMapper:     utils.NewDataTypeMapper(),
		stats:          newQueryStats(),
	}
}

func newQueryStats() *queryStats {
	return &queryStats{
		queriesByType: make(map[string]int64),
	}
}

func (qs *queryService) ExecuteQuery(ctx context.Context, req *model.QueryRequest) (*model.QueryResponse, error) {
	startTime := time.Now()

	// Validate request
	//if err := qs.ValidateQuery(ctx, req); err != nil {
	//	qs.recordQueryStats(req.DataSourceID, false, time.Since(startTime))
	//	return qs.createErrorResponse(err, req.SQL), nil
	//}

	// Get data source
	dataSource, err := qs.datasourceRepo.GetByID(ctx, req.DataSourceID)
	if err != nil {
		qs.recordQueryStats(req.DataSourceID, false, time.Since(startTime))
		return qs.createErrorResponse(err, req.SQL), nil
	}

	// Check if data source is active
	if dataSource.Status != model.DataSourceStatusActive {
		qs.recordQueryStats(req.DataSourceID, false, time.Since(startTime))
		return qs.createErrorResponse(fmt.Errorf("data source is %s", dataSource.Status), req.SQL), nil
	}

	// Get database connection
	db, err := qs.connPool.GetConnection(ctx, dataSource)
	if err != nil {
		qs.recordQueryStats(req.DataSourceID, false, time.Since(startTime))
		return qs.createErrorResponse(err, req.SQL), nil
	}

	// Execute query with timeout
	queryCtx, cancel := context.WithTimeout(ctx, time.Duration(req.Timeout)*time.Second)
	defer cancel()

	response, err := qs.executeQueryWithConnection(queryCtx, db, req, dataSource.Type)
	if err != nil {
		qs.recordQueryStats(req.DataSourceID, false, time.Since(startTime))
		// Mark data source as error if connection failed
		qs.datasourceRepo.SetError(ctx, req.DataSourceID)
		return qs.createErrorResponse(err, req.SQL), nil
	}

	// Record successful query stats
	qs.recordQueryStats(req.DataSourceID, true, time.Since(startTime))
	return response, nil
}

func (qs *queryService) ValidateQuery(ctx context.Context, req *model.QueryRequest) error {
	// Basic validation
	if !req.IsValid() {
		return fmt.Errorf("invalid query request")
	}

	// Apply defaults
	req.ApplyDefaults()

	// Get data source to check database type
	dataSource, err := qs.datasourceRepo.GetByID(ctx, req.DataSourceID)
	if err != nil {
		return err
	}

	// Validate SQL with data source-specific syntax checking
	if err := qs.sqlValidator.ValidateDataSourceSyntax(req.SQL, dataSource.Type); err != nil {
		return err
	}

	return nil
}

func (qs *queryService) GetQueryStats(ctx context.Context) (*model.QueryStats, error) {
	qs.stats.mutex.RLock()
	defer qs.stats.mutex.RUnlock()

	avgExecutionTime := 0.0
	if qs.stats.totalQueries > 0 {
		avgExecutionTime = float64(qs.stats.totalExecutionTime) / float64(qs.stats.totalQueries) / 1e9 // Convert to seconds
	}

	return &model.QueryStats{
		TotalQueries:      qs.stats.totalQueries,
		SuccessfulQueries: qs.stats.successfulQueries,
		FailedQueries:     qs.stats.failedQueries,
		AvgExecutionTime:  avgExecutionTime,
		LastQueryTime:     qs.stats.lastQueryTime,
		QueriesByType:     qs.stats.queriesByType,
	}, nil
}

// executeQueryWithConnection executes a query using the provided database connection
func (qs *queryService) executeQueryWithConnection(ctx context.Context, db *sql.DB, req *model.QueryRequest, dbType model.DatabaseType) (*model.QueryResponse, error) {
	// Prepare the query with LIMIT and OFFSET if not already present
	finalSQL := qs.applyPagination(req.SQL, req.Limit, req.Offset, dbType)

	// Execute query
	rows, err := db.QueryContext(ctx, finalSQL, req.Parameters...)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}
	defer rows.Close()

	// Get column information
	columns, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to get column types: %w", err)
	}

	// Build column information
	columnInfo := qs.buildColumnInfo(columns, dbType)

	// Process rows
	var results [][]interface{}
	rowCount := 0
	for rows.Next() {
		rowCount++
		row, err := qs.scanRow(rows, len(columns))
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Standardize row values
		standardizedRow, err := qs.standardizeRow(row, columnInfo)
		if err != nil {
			return nil, fmt.Errorf("failed to standardize row: %w", err)
		}

		results = append(results, standardizedRow)
	}

	// Check for errors from rows
	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	// Check if there might be more results
	hasMore := rowCount == req.Limit

	// Build response
	return &model.QueryResponse{
		Success: true,
		Columns: columnInfo,
		Rows:    results,
		Metadata: model.QueryMetadata{
			RowCount:        rowCount,
			ExecutionTimeMs: time.Since(time.Now()).Milliseconds(),
			DataSourceID:    req.DataSourceID,
			DatabaseType:    string(dbType),
			HasMore:         hasMore,
			Limit:           req.Limit,
			Offset:          req.Offset,
			ExecutedAt:      time.Now(),
		},
	}, nil
}

// applyPagination adds LIMIT and OFFSET to SQL if not already present
func (qs *queryService) applyPagination(sql string, limit, offset int, dbType model.DatabaseType) string {
	sqlUpper := strings.ToUpper(strings.TrimSpace(sql))

	// Check if LIMIT is already present
	if strings.Contains(sqlUpper, " LIMIT ") {
		return sql
	}

	// Handle different database types
	switch dbType {
	case model.DatabaseTypePostgreSQL:
		if offset > 0 {
			return fmt.Sprintf("%s LIMIT %d OFFSET %d", sql, limit, offset)
		}
		return fmt.Sprintf("%s LIMIT %d", sql, limit)

	case model.DatabaseTypeOracle:
		// Oracle uses different pagination syntax
		if offset > 0 {
			return fmt.Sprintf("SELECT * FROM (SELECT a.*, ROWNUM rnum FROM (%s) a WHERE ROWNUM <= %d) WHERE rnum > %d", sql, limit+offset, offset)
		}
		return fmt.Sprintf("SELECT * FROM (%s) WHERE ROWNUM <= %d", sql, limit)

	default: // MySQL, MariaDB
		if offset > 0 {
			return fmt.Sprintf("%s LIMIT %d OFFSET %d", sql, limit, offset)
		}
		return fmt.Sprintf("%s LIMIT %d", sql, limit)
	}
}

// buildColumnInfo builds column information from database column types
func (qs *queryService) buildColumnInfo(columns []*sql.ColumnType, dbType model.DatabaseType) []model.ColumnInfo {
	columnInfo := make([]model.ColumnInfo, len(columns))

	for i, col := range columns {
		nullable, _ := col.Nullable()

		columnInfo[i] = model.ColumnInfo{
			Name:     col.Name(),
			Type:     string(qs.typeMapper.MapToStandardType(dbType, col.DatabaseTypeName(), nullable)),
			Nullable: nullable,
		}
	}

	return columnInfo
}

// scanRow scans a single row from the database
func (qs *queryService) scanRow(rows *sql.Rows, columnCount int) ([]interface{}, error) {
	row := make([]interface{}, columnCount)
	rowPointers := make([]interface{}, columnCount)

	for i := range row {
		rowPointers[i] = &row[i]
	}

	if err := rows.Scan(rowPointers...); err != nil {
		return nil, err
	}

	return row, nil
}

// standardizeRow standardizes row values based on column types
func (qs *queryService) standardizeRow(row []interface{}, columns []model.ColumnInfo) ([]interface{}, error) {
	standardizedRow := make([]interface{}, len(row))

	for i, value := range row {
		if i >= len(columns) {
			standardizedRow[i] = value
			continue
		}

		targetType := model.StandardizedType(columns[i].Type)
		standardized, err := qs.typeMapper.StandardizeValue(value, targetType)
		if err != nil {
			return nil, fmt.Errorf("failed to standardize value for column %s: %w", columns[i].Name, err)
		}

		standardizedRow[i] = standardized
	}

	return standardizedRow, nil
}

// createErrorResponse creates an error response
func (qs *queryService) createErrorResponse(err error, sql string) *model.QueryResponse {
	var errorInfo *model.QueryError

	switch {
	case err == security.ErrNotSelectQuery:
		errorInfo = &model.QueryError{
			Code:    "SECURITY_VIOLATION",
			Message: "Only SELECT queries are allowed",
			SQL:     qs.maskSensitiveSQL(sql),
		}
	case err == security.ErrSQLInjection:
		errorInfo = &model.QueryError{
			Code:    "SECURITY_VIOLATION",
			Message: "Potential SQL injection detected",
			SQL:     qs.maskSensitiveSQL(sql),
		}
	case err == repository.ErrDataSourceNotFound:
		errorInfo = &model.QueryError{
			Code:    "DATASOURCE_NOT_FOUND",
			Message: "Data source not found",
		}
	default:
		errorInfo = &model.QueryError{
			Code:    "QUERY_EXECUTION_ERROR",
			Message: err.Error(),
			SQL:     qs.maskSensitiveSQL(sql),
		}
	}

	return &model.QueryResponse{
		Success: false,
		Error:   errorInfo,
	}
}

// maskSensitiveSQL masks potentially sensitive information in SQL for logging
func (qs *queryService) maskSensitiveSQL(sql string) string {
	// For now, just return the SQL as-is
	// In production, you might want to mask sensitive data
	return sql
}

// recordQueryStats records query execution statistics
func (qs *queryService) recordQueryStats(dataSourceID string, success bool, duration time.Duration) {
	qs.stats.mutex.Lock()
	defer qs.stats.mutex.Unlock()

	qs.stats.totalQueries++
	qs.stats.lastQueryTime = time.Now()
	qs.stats.totalExecutionTime += duration.Nanoseconds()

	if success {
		qs.stats.successfulQueries++
	} else {
		qs.stats.failedQueries++
	}

	// Count queries by data source type (would need to look up the data source)
	qs.stats.queriesByType[dataSourceID]++
}