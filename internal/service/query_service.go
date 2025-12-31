package service

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"nexus-gateway/internal/database"
	"nexus-gateway/internal/model"
	"nexus-gateway/internal/repository"
	"nexus-gateway/internal/security"
	"nexus-gateway/internal/utils"

	"github.com/google/uuid"
)

type QueryService interface {
	ExecuteQuery(ctx context.Context, req *model.QueryRequest) (*model.QueryResponse, error)
	ValidateQuery(ctx context.Context, req *model.QueryRequest) error
	GetQueryStats(ctx context.Context) (*model.QueryStats, error)
	FetchQuery(ctx context.Context, req *model.FetchQueryRequest) (*model.FetchQueryResponse, error)
	FetchNextBatch(ctx context.Context, queryID, slug, token string, batchSize int) (*model.FetchQueryResponse, error)
}

// QuerySession represents a query session for batch fetching
type QuerySession struct {
	QueryID      string
	Slug         string
	Token        string
	DataSourceID string
	SQL          string
	Type         int
	BatchSize    int
	Cursor       interface{} // Cursor for pagination
	TotalRows    int64
	FetchedRows  int64
	Columns      []model.ColumnInfo
	Entries      [][]string
	CreatedAt    time.Time
	ExpiresAt    time.Time
	mutex        sync.RWMutex
}

// SessionManager manages query sessions
type SessionManager struct {
	sessions map[string]*QuerySession
	mutex    sync.RWMutex
	ttl      time.Duration
}

type queryService struct {
	datasourceRepo  repository.DataSourceRepository
	connPool        *database.ConnectionPool
	sqlValidator    *security.SQLValidator
	typeMapper      *utils.DataTypeMapper
	stats           *queryStats
	sessionManager  *SessionManager
	preferStreaming bool
}

type queryStats struct {
	totalQueries       int64
	successfulQueries  int64
	failedQueries      int64
	totalExecutionTime int64 // in nanoseconds
	lastQueryTime      time.Time
	queriesByType      map[string]int64
	mutex              sync.RWMutex
}

// NewQueryService creates a new instance of QueryService
func NewQueryService(datasourceRepo repository.DataSourceRepository, connPool *database.ConnectionPool, preferStreaming bool) QueryService {
	return &queryService{
		datasourceRepo:  datasourceRepo,
		connPool:        connPool,
		sqlValidator:    security.NewSQLValidator(10000),
		typeMapper:      utils.NewDataTypeMapper(),
		stats:           newQueryStats(),
		sessionManager:  NewSessionManager(),
		preferStreaming: preferStreaming,
	}
}

// NewSessionManager creates a new session manager
func NewSessionManager() *SessionManager {
	sm := &SessionManager{
		sessions: make(map[string]*QuerySession),
		ttl:      30 * time.Minute, // Session TTL: 30 minutes
	}

	// Start cleanup goroutine
	go sm.cleanupSessions()

	return sm
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

// FetchQuery executes a fetch query and returns the first batch of data
func (qs *queryService) FetchQuery(ctx context.Context, req *model.FetchQueryRequest) (*model.FetchQueryResponse, error) {
	// Validate SQL
	if err := qs.sqlValidator.ValidateStatement(req.SQL); err != nil {
		return nil, fmt.Errorf("SQL validation failed: %w", err)
	}

	// Get data source
	datasource, err := qs.datasourceRepo.GetByID(ctx, req.DataSourceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get data source: %w", err)
	}

	// Get connection from pool
	conn, err := qs.connPool.GetConnection(ctx, datasource)
	if err != nil {
		return nil, fmt.Errorf("failed to get database connection: %w", err)
	}

	// Create query session
	session := &QuerySession{
		QueryID:      generateSessionID(),
		Slug:         generateSlug(),
		Token:        generateToken(),
		DataSourceID: req.DataSourceID,
		SQL:          req.SQL,
		Type:         req.Type,
		BatchSize:    req.BatchSize,
		CreatedAt:    time.Now(),
		ExpiresAt:    time.Now().Add(30 * time.Minute),
	}

	// Execute query and get first batch
	columns, entries, totalCount, err := qs.executeBatchQuery(ctx, conn, req.SQL, int64(req.BatchSize), 0)
	if err != nil {
		return nil, fmt.Errorf("failed to execute batch query: %w", err)
	}

	// Store session
	qs.sessionManager.Store(session)

	// Update session data
	session.Columns = columns
	session.Entries = entries
	session.TotalRows = totalCount
	session.FetchedRows = int64(len(entries))

	// Prepare response
	response := &model.FetchQueryResponse{
		QueryID:    session.QueryID,
		Slug:       session.Slug,
		Token:      session.Token,
		NextURI:    qs.buildNextURI(session.QueryID, session.Slug, session.Token, req.BatchSize),
		Columns:    columns,
		Entries:    entries,
		TotalCount: int(totalCount),
	}

	return response, nil
}

// FetchNextBatch fetches the next batch of data for a query session
func (qs *queryService) FetchNextBatch(ctx context.Context, queryID, slug, token string, batchSize int) (*model.FetchQueryResponse, error) {
	// Get session
	session, err := qs.sessionManager.Get(queryID)
	if err != nil {
		return nil, fmt.Errorf("session not found or expired")
	}

	// Verify slug and token
	if session.Slug != slug || session.Token != token {
		return nil, fmt.Errorf("invalid session credentials")
	}

	// Check if all data has been fetched
	if session.FetchedRows >= session.TotalRows {
		return &model.FetchQueryResponse{
			QueryID:    session.QueryID,
			Slug:       session.Slug,
			Token:      session.Token,
			Columns:    session.Columns,
			Entries:    [][]string{},
			TotalCount: int(session.TotalRows),
		}, nil
	}

	// Get data source and connection
	datasource, err := qs.datasourceRepo.GetByID(ctx, session.DataSourceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get data source: %w", err)
	}

	conn, err := qs.connPool.GetConnection(ctx, datasource)
	if err != nil {
		return nil, fmt.Errorf("failed to get database connection: %w", err)
	}

	// Calculate offset for next batch
	offset := session.FetchedRows

	// Execute query for next batch
	columns, entries, totalCount, err := qs.executeBatchQuery(ctx, conn, session.SQL, int64(batchSize), offset)
	if err != nil {
		return nil, fmt.Errorf("failed to execute batch query: %w", err)
	}

	// Update session
	session.Entries = entries
	session.FetchedRows += int64(len(entries))

	// Check if this is the last batch
	var nextURI string
	if session.FetchedRows < totalCount {
		nextURI = qs.buildNextURI(session.QueryID, session.Slug, session.Token, batchSize)
	}

	// Prepare response
	response := &model.FetchQueryResponse{
		QueryID:    session.QueryID,
		Slug:       session.Slug,
		Token:      session.Token,
		NextURI:    nextURI,
		Columns:    columns,
		Entries:    entries,
		TotalCount: int(totalCount),
	}

	return response, nil
}

// SessionManager methods

func (sm *SessionManager) Store(session *QuerySession) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	sm.sessions[session.QueryID] = session
}

func (sm *SessionManager) Get(queryID string) (*QuerySession, error) {
	sm.mutex.RLock()
	defer sm.mutex.RUnlock()

	session, exists := sm.sessions[queryID]
	if !exists {
		return nil, fmt.Errorf("session not found")
	}

	// Check if session is expired
	if time.Now().After(session.ExpiresAt) {
		return nil, fmt.Errorf("session expired")
	}

	return session, nil
}

func (sm *SessionManager) Delete(queryID string) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	delete(sm.sessions, queryID)
}

func (sm *SessionManager) cleanupSessions() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		sm.mutex.Lock()
		now := time.Now()
		for id, session := range sm.sessions {
			if now.After(session.ExpiresAt) {
				delete(sm.sessions, id)
			}
		}
		sm.mutex.Unlock()
	}
}

// Helper functions

func generateSessionID() string {
	return uuid.New().String()
}

func generateSlug() string {
	return uuid.New().String()[:8]
}

func generateToken() string {
	return uuid.New().String()
}

func (qs *queryService) buildNextURI(queryID, slug, token string, batchSize int) string {
	return fmt.Sprintf("/api/v1/fetch/%s/%s/%s?batch_size=%d", queryID, slug, token, batchSize)
}

// executeBatchQuery executes a query and returns results in batch format
func (qs *queryService) executeBatchQuery(ctx context.Context, conn interface{}, query string, batchSize, offset int64) ([]model.ColumnInfo, [][]string, int64, error) {
	// Convert conn to *sql.DB
	db, ok := conn.(*sql.DB)
	if !ok {
		return nil, nil, 0, fmt.Errorf("invalid database connection type")
	}

	// Apply pagination to the SQL query
	paginatedSQL := qs.applyBatchPagination(query, batchSize, offset)

	// Execute the query
	rows, err := db.QueryContext(ctx, paginatedSQL)
	if err != nil {
		return nil, nil, 0, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Get column information
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, 0, fmt.Errorf("failed to get column types: %w", err)
	}

	// Build column information
	columns := qs.buildBatchColumnInfo(columnTypes)

	// Process rows
	var entries [][]string
	for rows.Next() {
		rowValues, err := qs.scanBatchRow(rows, len(columnTypes))
		if err != nil {
			return nil, nil, 0, fmt.Errorf("failed to scan row: %w", err)
		}
		entries = append(entries, rowValues)
	}

	// Check for errors from rows iteration
	if err = rows.Err(); err != nil {
		return nil, nil, 0, fmt.Errorf("error iterating rows: %w", err)
	}

	// For total count, we need to run a separate COUNT query
	totalCount, err := qs.getRowCount(ctx, db, query)
	if err != nil {
		// If count query fails, use the current row count as an estimate
		totalCount = offset + int64(len(entries))
	}

	return columns, entries, totalCount, nil
}

// applyBatchPagination applies pagination to SQL query for batch fetching
func (qs *queryService) applyBatchPagination(sql string, batchSize, offset int64) string {
	sql = strings.TrimSpace(sql)

	// Check if query already has LIMIT or OFFSET
	sqlUpper := strings.ToUpper(sql)
	if strings.Contains(sqlUpper, " LIMIT ") || strings.Contains(sqlUpper, " OFFSET ") {
		// For complex queries with existing pagination, add a subquery wrapper
		return qs.applySubqueryPagination(sql, batchSize, offset)
	}

	// Use standard LIMIT/OFFSET for most databases
	return qs.applyStandardPagination(sql, batchSize, offset)
}

// applyStandardPagination applies standard LIMIT/OFFSET pagination
func (qs *queryService) applyStandardPagination(sql string, batchSize, offset int64) string {
	if offset > 0 {
		return fmt.Sprintf("%s LIMIT %d OFFSET %d", sql, batchSize, offset)
	}
	return fmt.Sprintf("%s LIMIT %d", sql, batchSize)
}

// applyPostgreSQLPagination applies PostgreSQL-specific pagination
func (qs *queryService) applyPostgreSQLPagination(sql string, batchSize, offset int64) string {
	if offset > 0 {
		return fmt.Sprintf("%s LIMIT %d OFFSET %d", sql, batchSize, offset)
	}
	return fmt.Sprintf("%s LIMIT %d", sql, batchSize)
}

// applyOraclePagination applies Oracle-specific pagination using ROWNUM
func (qs *queryService) applyOraclePagination(sql string, batchSize, offset int64) string {
	if offset > 0 {
		return fmt.Sprintf("SELECT * FROM (SELECT a.*, ROWNUM rnum FROM (%s) a WHERE ROWNUM <= %d) WHERE rnum > %d", sql, batchSize+offset, offset)
	}
	return fmt.Sprintf("SELECT * FROM (%s) WHERE ROWNUM <= %d", sql, batchSize)
}

// applyGaussDBPagination applies GaussDB-specific pagination
func (qs *queryService) applyGaussDBPagination(sql string, batchSize, offset int64) string {
	// GaussDB may use standard SQL syntax or have specific requirements
	// For now, using standard pagination but this can be enhanced based on specific needs
	return qs.applyStandardPagination(sql, batchSize, offset)
}

// applyKingBasePagination applies KingBaseES-specific pagination
func (qs *queryService) applyKingBasePagination(sql string, batchSize, offset int64) string {
	// KingBaseES is a Chinese database that typically supports standard SQL syntax
	// but may have specific requirements for Chinese character handling
	return qs.applyStandardPagination(sql, batchSize, offset)
}

// applyOceanBasePagination applies OceanBase-specific pagination
func (qs *queryService) applyOceanBasePagination(sql string, batchSize, offset int64) string {
	// OceanBase is a distributed database that may have specific pagination needs
	// Using standard SQL syntax for now, but can be enhanced for OceanBase specific features
	return qs.applyStandardPagination(sql, batchSize, offset)
}

// applyDMPagination applies DM (达梦) database specific pagination
func (qs *queryService) applyDMPagination(sql string, batchSize, offset int64) string {
	// DM database is a Chinese database with specific syntax requirements
	// Using standard SQL syntax for now, but can be enhanced for DM specific features
	return qs.applyStandardPagination(sql, batchSize, offset)
}

// applySubqueryPagination applies pagination with subquery wrapper for complex queries
func (qs *queryService) applySubqueryPagination(sql string, batchSize, offset int64) string {
	// Use a standard approach with LIMIT/OFFSET for simplicity
	return fmt.Sprintf("SELECT * FROM (%s) AS batch_query LIMIT %d OFFSET %d", sql, batchSize, offset)
}

// buildBatchColumnInfo builds column information from column types
func (qs *queryService) buildBatchColumnInfo(columnTypes []*sql.ColumnType) []model.ColumnInfo {
	columns := make([]model.ColumnInfo, len(columnTypes))

	for i, col := range columnTypes {
		nullable, _ := col.Nullable()
		columns[i] = model.ColumnInfo{
			Name:     col.Name(),
			Type:     string(qs.typeMapper.MapToStandardType(model.DatabaseTypeMySQL, col.DatabaseTypeName(), nullable)),
			Nullable: nullable,
		}
	}

	return columns
}

// mapColumnTypeToStandard maps database-specific type names to standard types
// Extended to better handle Chinese database types
func (qs *queryService) mapColumnTypeToStandard(dbTypeName string) string {
	dbTypeUpper := strings.ToUpper(dbTypeName)

	switch dbTypeUpper {
	// Integer types
	case "INT", "INTEGER", "TINYINT", "SMALLINT", "MEDIUMINT", "BIGINT",
		"INT4", "INT8", "INT2", "SERIAL", "BIGSERIAL", "SMALLSERIAL",
		"INTEGER UNSIGNED", "BIGINT UNSIGNED", "SMALLINT UNSIGNED",
		"NUMBER", "DOUBLE PRECISION":
		return "integer"

	// Floating point types
	case "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL":
		return "decimal"

	// String types - including Chinese character set handling
	case "VARCHAR", "CHAR", "TEXT", "LONGTEXT", "MEDIUMTEXT", "TINYTEXT",
		"NVARCHAR", "NCHAR", "NTEXT", "VARCHAR2", "CHAR2", "VARCHARC", "CHARC",
		"CLOB", "NCLOB", "LONG", "LONG RAW",
		"TEXT(n)", "VARCHAR(n)", "NVARCHAR(n)", "VARCHAR2(n)":
		return "string"

	// Date/time types
	case "DATE", "TIME", "DATETIME", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE",
		"DATE ONLY", "TIME ONLY", "DATETIME2", "SMALLDATETIME", "DATETIMEOFFSET":
		return "datetime"

	// Boolean types
	case "BOOLEAN", "BOOL", "BIT", "TINYINT(1)", "YESNO":
		return "boolean"

	// Binary types
	case "BINARY", "VARBINARY", "BLOB", "BYTEA", "RAW", "BLOB(n)", "BINARY(n)":
		return "binary"

	// JSON types
	case "JSON", "JSONB", "JSONB(n)", "JSONC":
		return "json"

	// UUID types
	case "UUID", "UNIQUEIDENTIFIER", "GUID":
		return "uuid"

	// Chinese database specific types
	case "TIMESTAMP6", "TIMESTAMP WITH LOCAL TIME ZONE":
		return "datetime"
	case "NUMBER(n)", "NUMBER(n,m)":
		return "decimal"
	case "VARCHARC(n)", "CHARC(n)":
		return "string"

	default:
		// For unknown types, return string but log for future enhancement
		return "string"
	}
}

// scanBatchRow scans a single row and returns string values
func (qs *queryService) scanBatchRow(rows *sql.Rows, columnCount int) ([]string, error) {
	// Create slice of interface{} pointers for scanning
	scanArgs := make([]interface{}, columnCount)
	values := make([]interface{}, columnCount)
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// Scan the row
	if err := rows.Scan(scanArgs...); err != nil {
		return nil, err
	}

	// Convert values to strings
	result := make([]string, columnCount)
	for i, value := range values {
		if value == nil {
			result[i] = "NULL"
		} else {
			result[i] = fmt.Sprintf("%v", value)
		}
	}

	return result, nil
}

// getRowCount gets the total row count for the query
func (qs *queryService) getRowCount(ctx context.Context, db *sql.DB, sql string) (int64, error) {
	// Remove ORDER BY clauses for count query as they don't affect the count
	countSQL := strings.ToUpper(sql)
	countSQL = regexp.MustCompile(`ORDER BY.*`).ReplaceAllString(countSQL, "")

	// Replace SELECT ... with SELECT COUNT(*)
	countSQL = regexp.MustCompile(`SELECT\s+(.*?)\s+FROM`).ReplaceAllString(countSQL, "SELECT COUNT(*) FROM")

	// Add database-specific limit for complex queries
	if strings.Contains(countSQL, "GROUP BY") || strings.Contains(countSQL, "DISTINCT") {
		// For now, use a general limit, but this could be database-specific
		countSQL += " LIMIT 1000000"
	}

	var count int64
	err := db.QueryRowContext(ctx, countSQL).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to count rows: %w", err)
	}

	return count, nil
}
