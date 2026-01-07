package unifiedservice

import (
	"context"
	"fmt"
	"sync"
	"time"

	"nexus-gateway/internal/config"
	"nexus-gateway/internal/model"
	service "nexus-gateway/internal/service"

	"github.com/google/uuid"
)

// QueryService interface that UnifiedQueryService implements
type QueryService interface {
	ExecuteQuery(ctx context.Context, req *model.QueryRequest) (*model.QueryResponse, error)
	ValidateQuery(ctx context.Context, req *model.QueryRequest) error
	GetQueryStats(ctx context.Context) (*model.QueryStats, error)
	FetchQuery(ctx context.Context, req *model.FetchQueryRequest) (*model.FetchQueryResponse, error)
	FetchNextBatch(ctx context.Context, queryID, slug, token string, batchSize int) (*model.FetchQueryResponse, error)
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

// UnifiedQueryService provides a unified interface that can use either pagination or streaming
// based on configuration, but always returns the same response format
type UnifiedQueryService struct {
	paginationService       service.QueryService // The original QueryService implementation
	streamingService        *service.StreamingService
	config                  *config.Config
	streamingSessionManager *StreamingSessionManager
}

// NewUnifiedQueryService creates a new unified query service that chooses between
// pagination and streaming based on configuration
func NewUnifiedQueryService(
	paginationService service.QueryService,
	streamingService *service.StreamingService,
	config *config.Config,
) *UnifiedQueryService {
	return &UnifiedQueryService{
		paginationService:       paginationService,
		streamingService:        streamingService,
		config:                  config,
		streamingSessionManager: NewStreamingSessionManager(),
	}
}

// ExecuteQuery executes a query using either pagination or streaming based on configuration,
// but always returns the same response format (model.QueryResponse)
func (uqs *UnifiedQueryService) ExecuteQuery(ctx context.Context, req *model.QueryRequest) (*model.QueryResponse, error) {
	// Determine execution mode based on configuration
	executionMode := uqs.getExecutionMode()

	switch executionMode {
	case "streaming":
		return uqs.executeWithStreaming(ctx, req)
	case "pagination":
		return uqs.executeWithPagination(ctx, req)
	case "auto":
		// For auto mode, we can implement logic to choose based on query characteristics
		// For now, default to pagination for compatibility
		return uqs.executeWithPagination(ctx, req)
	default:
		// Default to pagination
		return uqs.executeWithPagination(ctx, req)
	}
}

// ValidateQuery validates the query using the pagination service's validator
func (uqs *UnifiedQueryService) ValidateQuery(ctx context.Context, req *model.QueryRequest) error {
	return uqs.paginationService.ValidateQuery(ctx, req)
}

// GetQueryStats returns query statistics
func (uqs *UnifiedQueryService) GetQueryStats(ctx context.Context) (*model.QueryStats, error) {
	return uqs.paginationService.GetQueryStats(ctx)
}

// FetchQuery executes a fetch query using the pagination service
func (uqs *UnifiedQueryService) FetchQuery(ctx context.Context, req *model.FetchQueryRequest) (*model.FetchQueryResponse, error) {
	// Determine execution mode based on configuration
	executionMode := uqs.getExecutionMode()

	switch executionMode {
	case "streaming":
		return uqs.fetchWithStreaming(ctx, req)
	case "pagination":
		return uqs.paginationService.FetchQuery(ctx, req)
	case "auto":
		// Default to pagination for compatibility
		return uqs.paginationService.FetchQuery(ctx, req)
	default:
		// Default to pagination
		return uqs.paginationService.FetchQuery(ctx, req)
	}
}

// FetchNextBatch fetches the next batch using the pagination service
func (uqs *UnifiedQueryService) FetchNextBatch(ctx context.Context, queryID, slug, token string, batchSize int) (*model.FetchQueryResponse, error) {
	// Determine execution mode based on configuration
	executionMode := uqs.getExecutionMode()

	switch executionMode {
	case "streaming":
		return uqs.fetchNextBatchWithStreaming(ctx, queryID, slug, token, batchSize)
	case "pagination":
		return uqs.paginationService.FetchNextBatch(ctx, queryID, slug, token, batchSize)
	case "auto":
		// Default to pagination for compatibility
		return uqs.paginationService.FetchNextBatch(ctx, queryID, slug, token, batchSize)
	default:
		// Default to pagination
		return uqs.paginationService.FetchNextBatch(ctx, queryID, slug, token, batchSize)
	}
}

// ExecuteStreamingQuery executes a query using streaming approach
func (uqs *UnifiedQueryService) ExecuteStreamingQuery(ctx context.Context, req *model.QueryRequest) (*model.QueryResponse, error) {
	return uqs.executeWithStreaming(ctx, req)
}

// getExecutionMode determines the execution mode based on configuration
func (uqs *UnifiedQueryService) getExecutionMode() string {
	if uqs.config != nil && uqs.config.Query.ExecutionMode != "" {
		return uqs.config.Query.ExecutionMode
	}
	// Default to auto mode
	return "auto"
}

// executeWithPagination executes the query using the pagination approach
func (uqs *UnifiedQueryService) executeWithPagination(ctx context.Context, req *model.QueryRequest) (*model.QueryResponse, error) {
	return uqs.paginationService.ExecuteQuery(ctx, req)
}

// executeWithStreaming executes the query using the streaming approach but converts
// the result to the standard QueryResponse format
func (uqs *UnifiedQueryService) executeWithStreaming(ctx context.Context, req *model.QueryRequest) (*model.QueryResponse, error) {
	// Execute streaming query
	streamID, err := uqs.streamingService.ExecuteStreamQuery(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to start streaming query: %w", err)
	}

	// Fetch all chunks and aggregate them into a single QueryResponse
	var allRows [][]interface{}
	var columns []model.ColumnInfo
	var hasMore bool
	var totalRows int64

	firstChunk := true
	for {
		chunk, err := uqs.streamingService.FetchStreamChunk(ctx, streamID)
		if err != nil {
			// Try to close the stream even if there was an error
			uqs.streamingService.CloseStream(streamID)
			return nil, fmt.Errorf("failed to fetch stream chunk: %w", err)
		}

		if firstChunk {
			columns = chunk.Columns
			firstChunk = false
		}

		// Add rows from this chunk
		allRows = append(allRows, chunk.Rows...)
		totalRows += int64(len(chunk.Rows))
		hasMore = chunk.HasMore

		// If no more data, break
		if !hasMore {
			break
		}
	}

	// Close the stream
	_ = uqs.streamingService.CloseStream(streamID)

	// Create response in the same format as pagination service
	startTime := time.Now()
	response := &model.QueryResponse{
		Success: true,
		Columns: columns,
		Rows:    allRows,
		Metadata: model.QueryMetadata{
			RowCount:        len(allRows),
			ExecutionTimeMs: time.Since(startTime).Milliseconds(),
			DataSourceID:    req.DataSourceID,
			DatabaseType:    "unknown", // Would need to get from data source
			HasMore:         false,     // Since we've fetched all data
			Limit:           req.Limit,
			Offset:          req.Offset,
			ExecutedAt:      time.Now(),
		},
	}

	return response, nil
}

// fetchWithStreaming executes a fetch query using the streaming approach
func (uqs *UnifiedQueryService) fetchWithStreaming(ctx context.Context, req *model.FetchQueryRequest) (*model.FetchQueryResponse, error) {
	// Create a new streaming session
	session := &StreamingSession{
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

	// Execute streaming query to get the stream ID
	streamReq := &model.QueryRequest{
		DataSourceID: req.DataSourceID,
		SQL:          req.SQL,
		Limit:        req.BatchSize,
		Offset:       0,
		Timeout:      req.Timeout,
	}
	streamID, err := uqs.streamingService.ExecuteStreamQuery(ctx, streamReq)
	if err != nil {
		return nil, fmt.Errorf("failed to start streaming query: %w", err)
	}

	session.StreamID = streamID

	// Fetch the first batch of data
	chunk, err := uqs.streamingService.FetchStreamChunk(ctx, streamID)
	if err != nil {
		// Try to close the stream even if there was an error
		uqs.streamingService.CloseStream(streamID)
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

	// Update session data
	session.Columns = columns
	session.Entries = entries
	session.TotalRows = int64(len(entries)) // This is an approximation, we'll improve this
	session.FetchedRows = int64(len(entries))

	// Store the session
	uqs.streamingSessionManager.Store(session)

	// Create a response similar to pagination service
	response := &model.FetchQueryResponse{
		QueryID:    session.QueryID,
		Slug:       session.Slug,
		Token:      session.Token,
		NextURI:    "", // We'll set this if there are more results
		Columns:    columns,
		Entries:    entries,
		TotalCount: int(session.TotalRows),
	}

	// Add NextURI if there are more results
	if chunk.HasMore {
		response.NextURI = uqs.buildNextURI(session.QueryID, session.Slug, session.Token, req.BatchSize)
	}

	return response, nil
}

// fetchNextBatchWithStreaming fetches the next batch using the streaming approach
func (uqs *UnifiedQueryService) fetchNextBatchWithStreaming(ctx context.Context, queryID, slug, token string, batchSize int) (*model.FetchQueryResponse, error) {
	// Get the streaming session
	session, err := uqs.streamingSessionManager.Get(queryID)
	if err != nil {
		return nil, fmt.Errorf("session not found or expired: %w", err)
	}

	// Verify slug and token
	if session.Slug != slug || session.Token != token {
		return nil, fmt.Errorf("invalid session credentials")
	}

	// Check if stream exists and is valid
	_, err = uqs.streamingService.GetStreamStatus(session.StreamID)
	if err != nil {
		return nil, fmt.Errorf("stream not found or expired: %w", err)
	}

	// Fetch the next chunk of data
	chunk, err := uqs.streamingService.FetchStreamChunk(ctx, session.StreamID)
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

	// Create a response similar to pagination service
	response := &model.FetchQueryResponse{
		QueryID:    session.QueryID,
		Slug:       session.Slug,
		Token:      session.Token,
		Columns:    columns,
		Entries:    entries,
		TotalCount: int(session.TotalRows), // This is an approximation
	}

	// Add NextURI if there are more results
	if chunk.HasMore {
		response.NextURI = uqs.buildNextURI(session.QueryID, session.Slug, session.Token, batchSize)
	}

	return response, nil
}

// buildNextURI builds the next URI for fetching more data
func (uqs *UnifiedQueryService) buildNextURI(queryID, slug, token string, batchSize int) string {
	return fmt.Sprintf("/api/v1/fetch/%s/%s/%s?batch_size=%d", queryID, slug, token, batchSize)
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
