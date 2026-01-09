package service

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"nexus-gateway/internal/repository"
	"strings"
	"sync"
	"time"

	"nexus-gateway/internal/database"
	"nexus-gateway/internal/model"

	"github.com/google/uuid"
)

type StreamingService interface {
	//ExecuteQuery(ctx context.Context, req *model.QueryRequest) (*model.QueryResponse, error)
	//ValidateQuery(ctx context.Context, req *model.QueryRequest) error
	//GetQueryStats(ctx context.Context) (*model.QueryStats, error)
	FetchWithStreaming(ctx context.Context, req *model.FetchQueryRequest) (*model.FetchQueryResponse, error)
	FetchNextBatchWithStreaming(ctx context.Context, queryID, slug, token string, batchSize int) (*model.FetchQueryResponse, error)
}

// StreamingSession represents a streaming query session for batch fetching
type StreamingSession struct {
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
	Entries      []*[]interface{}
	CreatedAt    time.Time
	ExpiresAt    time.Time
	mutex        sync.RWMutex
	StreamID     string // Associated stream ID
}

// StreamingSessionManager manages streaming query sessions
type StreamingSessionManager struct {
	sessions map[string]*StreamingSession
	mutex    sync.RWMutex
	ttl      time.Duration
}

// NewStreamingSessionManager creates a new streaming session manager
func NewStreamingSessionManager() *StreamingSessionManager {
	sm := &StreamingSessionManager{
		sessions: make(map[string]*StreamingSession),
		ttl:      30 * time.Minute, // Session TTL: 30 minutes
	}

	// Start cleanup goroutine
	go sm.cleanupSessions()

	return sm
}

// Store stores a streaming session
func (sm *StreamingSessionManager) Store(session *StreamingSession) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	sm.sessions[session.QueryID] = session
}

// Get retrieves a streaming session
func (sm *StreamingSessionManager) Get(queryID string) (*StreamingSession, error) {
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

// Delete removes a streaming session
func (sm *StreamingSessionManager) Delete(queryID string) {
	sm.mutex.Lock()
	defer sm.mutex.Unlock()

	delete(sm.sessions, queryID)
}

// cleanupSessions periodically removes expired sessions
func (sm *StreamingSessionManager) cleanupSessions() {
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

type streamingService struct {
	datasourceRepo          repository.DataSourceRepository
	connPool                *database.ConnectionPool
	activeStreams           map[string]*ActiveStream
	streamsMutex            sync.RWMutex
	maxStreams              int
	streamTimeout           time.Duration
	streamingSessionManager *StreamingSessionManager
}

// ActiveStream represents an active streaming query
type ActiveStream struct {
	ID             string
	DataSourceID   string
	DatabaseType   model.DatabaseType
	Rows           *sql.Rows
	Columns        []model.ColumnInfo
	StartedAt      time.Time
	LastActivityAt time.Time
	RowsSent       int64
	BytesSent      int64
	CancelFunc     context.CancelFunc
	mutex          sync.Mutex
}

// StreamChunk represents a chunk of streamed data
type StreamChunk struct {
	StreamID string             `json:"streamId"`
	ChunkID  int64              `json:"chunkId"`
	Columns  []model.ColumnInfo `json:"columns,omitempty"` // Only in first chunk
	Rows     [][]interface{}    `json:"rows"`
	Metadata StreamMetadata     `json:"metadata"`
}

// StreamMetadata contains metadata about the stream
type StreamMetadata struct {
	RowCount      int64     `json:"rowCount"`
	ChunkSize     int       `json:"chunkSize"`
	ExecutionTime time.Time `json:"executionTime"`
}

// NewStreamingService creates a new streaming service
func NewStreamingService(datasourceRepo repository.DataSourceRepository, connPool *database.ConnectionPool) StreamingService {
	return &streamingService{
		datasourceRepo:          datasourceRepo,
		connPool:                connPool,
		activeStreams:           make(map[string]*ActiveStream),
		maxStreams:              100, // Max concurrent streams
		streamTimeout:           5 * time.Minute,
		streamingSessionManager: NewStreamingSessionManager(),
	}
}

// ExecuteStreamQuery executes a query and returns a stream ID
func (ss *streamingService) ExecuteStreamQuery(ctx context.Context, req *model.QueryRequest) (string, int64, error) {
	// Check stream limit
	if ss.getActiveStreamCount() >= ss.maxStreams {
		return "", 0, fmt.Errorf("maximum concurrent streams reached")
	}

	// Get data source
	// TODO: Implement data source retrieval
	dataSource, err := ss.datasourceRepo.GetByID(ctx, req.DataSourceID)
	if err != nil {
		return "", 0, fmt.Errorf("failed to get data source: %w", err)
	}

	// Create context with timeout
	streamCtx, cancel := context.WithTimeout(context.Background(), ss.streamTimeout)

	// Get connection
	db, err := ss.connPool.GetConnection(streamCtx, dataSource)
	if err != nil {
		cancel()
		return "", 0, fmt.Errorf("failed to get connection: %w", err)
	}

	// Execute query
	rows, err := db.QueryContext(streamCtx, req.SQL, req.Parameters...)
	if err != nil {
		cancel()
		return "", 0, fmt.Errorf("query execution failed: %w", err)
	}

	// Get column information
	columns, err := rows.ColumnTypes()
	if err != nil {
		rows.Close()
		cancel()
		return "", 0, fmt.Errorf("failed to get columns: %w", err)
	}

	// Build column info
	columnInfo := ss.buildColumnInfo(columns, dataSource.Type)

	// Create stream
	streamID := ss.generateStreamID()
	stream := &ActiveStream{
		ID:             streamID,
		DataSourceID:   req.DataSourceID,
		DatabaseType:   dataSource.Type,
		Rows:           rows,
		Columns:        columnInfo,
		StartedAt:      time.Now(),
		LastActivityAt: time.Now(),
		CancelFunc:     cancel,
	}

	// Register stream
	ss.streamsMutex.Lock()
	ss.activeStreams[streamID] = stream
	ss.streamsMutex.Unlock()

	totalCount, err := ss.getRowCount(ctx, db, req.SQL)
	if err != nil {
		return "", 0, fmt.Errorf("failed to get row count: %w", err)
	}

	return streamID, totalCount, nil
}

// FetchStreamChunk fetches the next chunk of data from a stream
func (ss *streamingService) FetchStreamChunk(ctx context.Context, streamID string, batchSize int) (*StreamChunk, error) {
	ss.streamsMutex.RLock()
	stream, exists := ss.activeStreams[streamID]
	ss.streamsMutex.RUnlock()

	if !exists {
		return nil, fmt.Errorf("stream not found: %s", streamID)
	}

	stream.mutex.Lock()
	defer stream.mutex.Unlock()

	// Check if stream is timed out
	if time.Since(stream.LastActivityAt) > ss.streamTimeout {
		ss.CloseStream(streamID)
		return nil, fmt.Errorf("stream timed out")
	}

	// Read chunk
	var rows [][]interface{}
	chunkRowCount := 0

	for chunkRowCount < batchSize {
		if !stream.Rows.Next() {
			// No more rows
			break
		}

		// Scan row
		columns := len(stream.Columns)
		row := make([]interface{}, columns)
		rowPointers := make([]interface{}, columns)

		for i := range row {
			rowPointers[i] = &row[i]
		}

		if err := stream.Rows.Scan(rowPointers...); err != nil {
			// Error scanning row - close stream
			ss.CloseStream(streamID)
			return nil, fmt.Errorf("error scanning row: %w", err)
		}

		// Standardize row values
		// TODO: Implement value standardization
		rows = append(rows, row)
		chunkRowCount++
	}

	// Check for errors
	if err := stream.Rows.Err(); err != nil {
		ss.CloseStream(streamID)
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	// Update stream stats
	stream.RowsSent += int64(chunkRowCount)
	stream.LastActivityAt = time.Now()

	return &StreamChunk{
		StreamID: streamID,
		ChunkID:  stream.RowsSent,
		Columns:  stream.Columns, // Include columns in every chunk for stateless clients
		Rows:     rows,
		Metadata: StreamMetadata{
			RowCount:      stream.RowsSent,
			ChunkSize:     batchSize,
			ExecutionTime: stream.StartedAt,
		},
	}, nil
}

// CloseStream closes an active stream
func (ss *streamingService) CloseStream(streamID string) error {
	ss.streamsMutex.Lock()
	defer ss.streamsMutex.Unlock()

	stream, exists := ss.activeStreams[streamID]
	if !exists {
		return fmt.Errorf("stream not found: %s", streamID)
	}

	// Close rows and cancel context
	if stream.Rows != nil {
		stream.Rows.Close()
	}
	if stream.CancelFunc != nil {
		stream.CancelFunc()
	}

	// Remove from active streams
	delete(ss.activeStreams, streamID)

	return nil
}

// GetStreamStatus returns the status of an active stream
func (ss *streamingService) GetStreamStatus(streamID string) (*ActiveStream, error) {
	ss.streamsMutex.RLock()
	defer ss.streamsMutex.RUnlock()

	stream, exists := ss.activeStreams[streamID]
	if !exists {
		return nil, fmt.Errorf("stream not found: %s", streamID)
	}

	// Return a copy to avoid race conditions
	return &ActiveStream{
		ID:             stream.ID,
		DataSourceID:   stream.DataSourceID,
		DatabaseType:   stream.DatabaseType,
		Columns:        stream.Columns,
		StartedAt:      stream.StartedAt,
		LastActivityAt: stream.LastActivityAt,
		RowsSent:       stream.RowsSent,
		BytesSent:      stream.BytesSent,
	}, nil
}

// GetAllStreams returns all active streams
func (ss *streamingService) GetAllStreams() []*ActiveStream {
	ss.streamsMutex.RLock()
	defer ss.streamsMutex.RUnlock()

	streams := make([]*ActiveStream, 0, len(ss.activeStreams))
	for _, stream := range ss.activeStreams {
		streams = append(streams, stream)
	}

	return streams
}

// CleanupInactiveStreams removes streams that have been inactive
func (ss *streamingService) CleanupInactiveStreams() int {
	ss.streamsMutex.Lock()
	defer ss.streamsMutex.Unlock()

	cleaned := 0
	now := time.Now()

	for streamID, stream := range ss.activeStreams {
		if now.Sub(stream.LastActivityAt) > ss.streamTimeout {
			// Close stream
			if stream.Rows != nil {
				stream.Rows.Close()
			}
			if stream.CancelFunc != nil {
				stream.CancelFunc()
			}
			delete(ss.activeStreams, streamID)
			cleaned++
		}
	}

	return cleaned
}

// StartCleanupRoutine starts a background routine to clean up inactive streams
func (ss *streamingService) StartCleanupRoutine(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cleaned := ss.CleanupInactiveStreams()
			if cleaned > 0 {
				// Log cleanup
			}
		}
	}
}

// Helper methods

func (ss *streamingService) generateStreamID() string {
	return fmt.Sprintf("stream_%d", time.Now().UnixNano())
}

func (ss *streamingService) getActiveStreamCount() int {
	ss.streamsMutex.RLock()
	defer ss.streamsMutex.RUnlock()
	return len(ss.activeStreams)
}

func (ss *streamingService) buildColumnInfo(columns []*sql.ColumnType, dbType model.DatabaseType) []model.ColumnInfo {
	columnInfo := make([]model.ColumnInfo, len(columns))

	for i, col := range columns {
		nullable, _ := col.Nullable()
		columnInfo[i] = model.ColumnInfo{
			Name:     col.Name(),
			Type:     col.DatabaseTypeName(),
			Nullable: nullable,
		}
	}

	return columnInfo
}

func (ss *streamingService) writeCSVHeader(writer io.Writer, columns []model.ColumnInfo) error {
	header := ""
	for i, col := range columns {
		if i > 0 {
			header += ","
		}
		header += col.Name
	}
	header += "\n"
	_, err := io.WriteString(writer, header)
	return err
}

func (ss *streamingService) writeCSVRows(writer io.Writer, rows [][]interface{}) error {
	for _, row := range rows {
		line := ""
		for i, val := range row {
			if i > 0 {
				line += ","
			}
			if val == nil {
				line += "NULL"
			} else {
				line += fmt.Sprintf("%v", val)
			}
		}
		line += "\n"
		if _, err := io.WriteString(writer, line); err != nil {
			return err
		}
	}
	return nil
}

// fetchWithStreaming executes a fetch query using the streaming approach
func (ss *streamingService) FetchWithStreaming(ctx context.Context, req *model.FetchQueryRequest) (*model.FetchQueryResponse, error) {
	// Create a new streaming session
	session := &StreamingSession{
		QueryID:      ss.generateSessionID(),
		Slug:         ss.generateSlug(),
		Token:        ss.generateToken(),
		DataSourceID: req.DataSourceID,
		SQL:          req.SQL,
		Type:         req.Type,
		BatchSize:    req.BatchSize,
		CreatedAt:    time.Now(),
		ExpiresAt:    time.Now().Add(30 * time.Minute),
	}

	// Execute streaming query to get the stream ID
	streamReq := &model.QueryRequest{
		DataSourceID: req.DataSourceID,
		SQL:          req.SQL,
		Limit:        req.BatchSize,
		Offset:       0,
		Timeout:      req.Timeout,
	}
	streamID, totalCount, err := ss.ExecuteStreamQuery(ctx, streamReq)
	if err != nil {
		return nil, fmt.Errorf("failed to start streaming query: %w", err)
	}

	// Fetch the first batch of data
	chunk, err := ss.FetchStreamChunk(ctx, streamID, req.BatchSize)
	if err != nil {
		// Try to close the stream even if there was an error
		ss.CloseStream(streamID)
		return nil, fmt.Errorf("failed to fetch stream chunk: %w", err)
	}

	// Convert chunk data to the expected format
	entries := make([]*[]interface{}, 0, len(chunk.Rows))
	for _, row := range chunk.Rows {
		entry := make([]interface{}, len(row))
		for j, val := range row {
			if val == nil {
				entry[j] = "NULL"
			} else if b, ok := val.([]byte); ok {
				entry[j] = string(b)
			} else {
				entry[j] = val
			}
		}
		entries = append(entries, &entry)
	}

	// Convert columns to the expected format
	columns := make([]model.ColumnInfo, len(chunk.Columns))
	for i, col := range chunk.Columns {
		columns[i] = model.ColumnInfo{
			Name:     col.Name,
			Type:     col.Type,
			Nullable: col.Nullable,
		}
	}

	// Create a response similar to pagination service
	response := &model.FetchQueryResponse{
		QueryID:    session.QueryID,
		Slug:       session.Slug,
		Token:      session.Token,
		NextURI:    "", // We'll set this if there are more results
		Columns:    columns,
		Entries:    entries,
		TotalCount: int(totalCount),
	}

	if req.Type == 2 && len(entries) < int(totalCount) {
		// Add NextURI if there are more results
		response.NextURI = ss.buildNextURI(session.QueryID, session.Slug, session.Token, req.BatchSize)

		// Update session data
		session.StreamID = streamID
		session.Columns = columns
		session.Entries = entries
		session.TotalRows = totalCount
		session.FetchedRows = int64(len(entries))

		// Store the session
		ss.streamingSessionManager.Store(session)

	} else {
		ss.CloseStream(streamID)
	}

	return response, nil
}

// fetchNextBatchWithStreaming fetches the next batch using the streaming approach
func (ss *streamingService) FetchNextBatchWithStreaming(ctx context.Context, queryID, slug, token string, batchSize int) (*model.FetchQueryResponse, error) {
	// Get the streaming session
	session, err := ss.streamingSessionManager.Get(queryID)
	if err != nil {
		return nil, fmt.Errorf("session not found or expired: %w", err)
	}

	// Verify slug and token
	if session.Slug != slug || session.Token != token {
		return nil, fmt.Errorf("invalid session credentials")
	}

	// Check if stream exists and is valid
	_, err = ss.GetStreamStatus(session.StreamID)
	if err != nil {
		return nil, fmt.Errorf("stream not found or expired: %w", err)
	}

	// Fetch the next chunk of data
	chunk, err := ss.FetchStreamChunk(ctx, session.StreamID, batchSize)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch stream chunk: %w", err)
	}

	// Convert chunk data to the expected format
	entries := make([]*[]interface{}, 0, len(chunk.Rows))
	for _, row := range chunk.Rows {
		entry := make([]interface{}, len(row))
		for j, val := range row {
			if val == nil {
				entry[j] = "NULL"
			} else if b, ok := val.([]byte); ok {
				entry[j] = string(b)
			} else {
				entry[j] = val
			}
		}
		entries = append(entries, &entry)
	}

	// Convert columns to the expected format (use the first chunk's columns if available)
	var columns []model.ColumnInfo
	if len(chunk.Columns) > 0 {
		// Use columns from the current chunk
		columns = make([]model.ColumnInfo, len(chunk.Columns))
		for i, col := range chunk.Columns {
			columns[i] = model.ColumnInfo{
				Name:     col.Name,
				Type:     col.Type,
				Nullable: col.Nullable,
			}
		}
	} else {
		// Use columns from the session
		columns = session.Columns
	}

	// Update session data
	session.Entries = entries
	session.FetchedRows += int64(len(entries))
	session.Token = ss.generateToken()

	// Create a response similar to pagination service
	response := &model.FetchQueryResponse{
		QueryID:    session.QueryID,
		Slug:       session.Slug,
		Token:      session.Token,
		Columns:    columns,
		Entries:    entries,
		TotalCount: int(session.TotalRows),
	}

	// Add NextURI if there are more results
	if session.FetchedRows < session.TotalRows {
		response.NextURI = ss.buildNextURI(session.QueryID, session.Slug, session.Token, batchSize)
	} else {
		// Close the stream if no more results
		ss.CloseStream(session.StreamID)
		ss.streamingSessionManager.Delete(session.QueryID)
	}

	return response, nil
}

// buildNextURI builds the next URI for fetching more data
func (ss *streamingService) buildNextURI(queryID, slug, token string, batchSize int) string {
	return fmt.Sprintf("/api/v1/fetch/%s/%s/%s?batch_size=%d", queryID, slug, token, batchSize)
}

// Helper functions

func (ss *streamingService) generateSessionID() string {
	return uuid.New().String()
}

func (ss *streamingService) generateSlug() string {
	return uuid.New().String()[:8]
}

func (ss *streamingService) generateToken() string {
	return uuid.New().String()
}

// getRowCount gets the total row count for the query
func (ss *streamingService) getRowCount(ctx context.Context, db *sql.DB, sql string) (int64, error) {
	// Remove ORDER BY clauses for count query as they don't affect the count
	// Use case-insensitive regex to preserve original case of identifiers
	countSQL := removeOrderByClause(sql)

	// For complex queries with GROUP BY or DISTINCT, use subquery approach
	upperSQL := strings.ToUpper(strings.TrimSpace(countSQL))
	if strings.Contains(upperSQL, "GROUP BY") || strings.Contains(upperSQL, "DISTINCT") {
		// Try standard ANSI SQL approach first (works for most databases)
		countSQL = fmt.Sprintf("SELECT COUNT(*) FROM (%s) AS t", countSQL)

		// Execute the count query
		var count int64
		err := db.QueryRowContext(ctx, countSQL).Scan(&count)
		if err != nil {
			// If the standard approach fails, try Oracle-style approach without alias
			countSQL = fmt.Sprintf("SELECT COUNT(*) FROM (%s)", removeOrderByClause(sql))
			err = db.QueryRowContext(ctx, countSQL).Scan(&count)
			if err != nil {
				return 0, fmt.Errorf("failed to count rows: %w", err)
			}
		}
		return count, nil
	} else {
		// For simpler queries, replace SELECT clause with COUNT(*)
		countSQL = replaceSelectWithCount(countSQL)

		var count int64
		err := db.QueryRowContext(ctx, countSQL).Scan(&count)
		if err != nil {
			return 0, fmt.Errorf("failed to count rows: %w", err)
		}

		return count, nil
	}
}
