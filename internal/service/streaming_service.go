package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"nexus-gateway/internal/database"
	"nexus-gateway/internal/model"
)

// StreamingService handles streaming query results for large datasets
type StreamingService struct {
	connPool      *database.ConnectionPool
	activeStreams map[string]*ActiveStream
	streamsMutex  sync.RWMutex
	maxStreams    int
	streamTimeout time.Duration
	chunkSize     int
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
	HasMore  bool               `json:"hasMore"`
	Metadata StreamMetadata     `json:"metadata"`
}

// StreamMetadata contains metadata about the stream
type StreamMetadata struct {
	RowCount      int64     `json:"rowCount"`
	ChunkSize     int       `json:"chunkSize"`
	ExecutionTime time.Time `json:"executionTime"`
}

// NewStreamingService creates a new streaming service
func NewStreamingService(connPool *database.ConnectionPool) *StreamingService {
	return &StreamingService{
		connPool:      connPool,
		activeStreams: make(map[string]*ActiveStream),
		maxStreams:    100, // Max concurrent streams
		streamTimeout: 5 * time.Minute,
		chunkSize:     1000, // Rows per chunk
	}
}

// ExecuteStreamQuery executes a query and returns a stream ID
func (ss *StreamingService) ExecuteStreamQuery(ctx context.Context, req *model.QueryRequest) (string, error) {
	// Check stream limit
	if ss.getActiveStreamCount() >= ss.maxStreams {
		return "", fmt.Errorf("maximum concurrent streams reached")
	}

	// Get data source
	// TODO: Implement data source retrieval
	dataSource := &model.DataSource{
		ID:   req.DataSourceID,
		Type: model.DatabaseTypeMySQL, // Placeholder
	}

	// Get connection
	db, err := ss.connPool.GetConnection(ctx, dataSource)
	if err != nil {
		return "", fmt.Errorf("failed to get connection: %w", err)
	}

	// Create context with timeout
	streamCtx, cancel := context.WithTimeout(ctx, ss.streamTimeout)

	// Execute query
	rows, err := db.QueryContext(streamCtx, req.SQL, req.Parameters...)
	if err != nil {
		cancel()
		return "", fmt.Errorf("query execution failed: %w", err)
	}

	// Get column information
	columns, err := rows.ColumnTypes()
	if err != nil {
		rows.Close()
		cancel()
		return "", fmt.Errorf("failed to get columns: %w", err)
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

	return streamID, nil
}

// FetchStreamChunk fetches the next chunk of data from a stream
func (ss *StreamingService) FetchStreamChunk(ctx context.Context, streamID string) (*StreamChunk, error) {
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

	for chunkRowCount < ss.chunkSize {
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
			stream.Rows.Close()
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
		stream.Rows.Close()
		ss.CloseStream(streamID)
		return nil, fmt.Errorf("row iteration error: %w", err)
	}

	// Update stream stats
	stream.RowsSent += int64(chunkRowCount)
	stream.LastActivityAt = time.Now()

	// Check if there are more rows
	hasMore := chunkRowCount == ss.chunkSize

	// If no more rows, close the stream
	if !hasMore {
		stream.Rows.Close()
		ss.CloseStream(streamID)
	}

	return &StreamChunk{
		StreamID: streamID,
		ChunkID:  stream.RowsSent,
		Columns:  stream.Columns, // Include columns in every chunk for stateless clients
		Rows:     rows,
		HasMore:  hasMore,
		Metadata: StreamMetadata{
			RowCount:      stream.RowsSent,
			ChunkSize:     ss.chunkSize,
			ExecutionTime: stream.StartedAt,
		},
	}, nil
}

// StreamToWriter streams query results directly to an io.Writer
func (ss *StreamingService) StreamToWriter(ctx context.Context, streamID string, writer io.Writer, format string) error {
	chunkID := int64(0)

	// Write opening based on format
	if format == "json" {
		encoder := json.NewEncoder(writer)
		if err := encoder.Encode(map[string]interface{}{"streamId": streamID, "status": "started"}); err != nil {
			return err
		}
	}

	for {
		chunk, err := ss.FetchStreamChunk(ctx, streamID)
		if err != nil {
			// Write error to stream
			if format == "json" {
				encoder := json.NewEncoder(writer)
				encoder.Encode(map[string]interface{}{"error": err.Error()})
			}
			return err
		}

		// Write chunk based on format
		if format == "json" {
			encoder := json.NewEncoder(writer)
			if err := encoder.Encode(chunk); err != nil {
				return err
			}
		} else if format == "csv" {
			// Write CSV format
			if chunkID == 0 {
				// Write header
				ss.writeCSVHeader(writer, chunk.Columns)
			}
			ss.writeCSVRows(writer, chunk.Rows)
		}

		chunkID++

		// Check if stream is complete
		if !chunk.HasMore {
			break
		}
	}

	// Write closing
	if format == "json" {
		encoder := json.NewEncoder(writer)
		encoder.Encode(map[string]interface{}{"status": "completed", "totalChunks": chunkID})
	}

	return nil
}

// CloseStream closes an active stream
func (ss *StreamingService) CloseStream(streamID string) error {
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
func (ss *StreamingService) GetStreamStatus(streamID string) (*ActiveStream, error) {
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
func (ss *StreamingService) GetAllStreams() []*ActiveStream {
	ss.streamsMutex.RLock()
	defer ss.streamsMutex.RUnlock()

	streams := make([]*ActiveStream, 0, len(ss.activeStreams))
	for _, stream := range ss.activeStreams {
		streams = append(streams, stream)
	}

	return streams
}

// CleanupInactiveStreams removes streams that have been inactive
func (ss *StreamingService) CleanupInactiveStreams() int {
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
func (ss *StreamingService) StartCleanupRoutine(ctx context.Context, interval time.Duration) {
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

func (ss *StreamingService) generateStreamID() string {
	return fmt.Sprintf("stream_%d", time.Now().UnixNano())
}

func (ss *StreamingService) getActiveStreamCount() int {
	ss.streamsMutex.RLock()
	defer ss.streamsMutex.RUnlock()
	return len(ss.activeStreams)
}

func (ss *StreamingService) buildColumnInfo(columns []*sql.ColumnType, dbType model.DatabaseType) []model.ColumnInfo {
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

func (ss *StreamingService) writeCSVHeader(writer io.Writer, columns []model.ColumnInfo) error {
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

func (ss *StreamingService) writeCSVRows(writer io.Writer, rows [][]interface{}) error {
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
