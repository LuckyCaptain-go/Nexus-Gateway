package table_formats

import (
	"context"
	"fmt"
	"time"
)

// HudiIncrementalQuery provides incremental query functionality for Hudi
type HudiIncrementalQuery struct {
	driver *HudiDriver
}

// NewHudiIncrementalQuery creates a new incremental query handler
func NewHudiIncrementalQuery(driver *HudiDriver) *HudiIncrementalQuery {
	return &HudiIncrementalQuery{driver: driver}
}

// IncrementalQueryRequest represents an incremental query request
type IncrementalQueryRequest struct {
	BasePath           string
	BeginInstantTime   string // Start instant (exclusive)
	EndInstantTime     string // End instant (inclusive)
	QueryType          string // "query_instant", "query_between_instants"
	SelectedFields     []string
	TableType          string // "COPY_ON_WRITE" or "MERGE_ON_READ"
}

// IncrementalQueryResult represents incremental query results
type IncrementalQueryResult struct {
	Data       [][]interface{} `json:"data"`
	Schema     []HudiField     `json:"schema"`
	BeginTime  string          `json:"beginTime"`
	EndTime    string          `json:"endTime"`
}

// QueryIncremental executes an incremental query
func (iq *HudiIncrementalQuery) QueryIncremental(ctx context.Context, req *IncrementalQueryRequest) (*IncrementalQueryResult, error) {
	// Build incremental query SQL
	sql := iq.buildIncrementalQuery(req)

	// Execute query
	result, err := iq.driver.restClient.QueryTable(ctx, sql)
	if err != nil {
		return nil, err
	}

	return &IncrementalQueryResult{
		Data:    result.Rows,
		Schema:  convertToHudiFields(result.Schema.Fields),
		BeginTime: req.BeginInstantTime,
		EndTime:   req.EndInstantTime,
	}, nil
}

// buildIncrementalQuery builds incremental query SQL
func (iq *HudiIncrementalQuery) buildIncrementalQuery(req *IncrementalQueryRequest) string {
	// Hudi incremental query syntax for Spark
	// SELECT * FROM table WHERE `_hoodie_commit_time` > 'beginTime' AND `_hoodie_commit_time` <= 'endTime'

	fields := "*"
	if len(req.SelectedFields) > 0 {
		fields = joinFields(req.SelectedFields)
	}

	whereClause := fmt.Sprintf("`_hoodie_commit_time` > '%s' AND `_hoodie_commit_time` <= '%s'",
		req.BeginInstantTime, req.EndInstantTime)

	return fmt.Sprintf("SELECT %s FROM %s WHERE %s", fields, req.BasePath, whereClause)
}

// GetChangesSince retrieves changes since a specific instant
func (iq *HudiIncrementalQuery) GetChangesSince(ctx context.Context, basePath, beginInstantTime string) (*IncrementalQueryResult, error) {
	// Get latest commit to use as end time
	latestCommit, err := iq.driver.GetLatestCommit(ctx, basePath)
	if err != nil {
		return nil, err
	}

	req := &IncrementalQueryRequest{
		BasePath:         basePath,
		BeginInstantTime: beginInstantTime,
		EndInstantTime:   latestCommit.CommitTime,
		QueryType:        "query_between_instants",
	}

	return iq.QueryIncremental(ctx, req)
}

// GetChangesInRange retrieves changes within a time range
func (iq *HudiIncrementalQuery) GetChangesInRange(ctx context.Context, basePath, beginInstantTime, endInstantTime string) (*IncrementalQueryResult, error) {
	req := &IncrementalQueryRequest{
		BasePath:         basePath,
		BeginInstantTime: beginInstantTime,
		EndInstantTime:   endInstantTime,
		QueryType:        "query_between_instants",
	}

	return iq.QueryIncremental(ctx, req)
}

// GetChangesSinceTimestamp retrieves changes since a timestamp
func (iq *HudiIncrementalQuery) GetChangesSinceTimestamp(ctx context.Context, basePath string, timestamp time.Time) (*IncrementalQueryResult, error) {
	instantTime := iq.driver.parser.FormatInstantTime(timestamp)
	return iq.GetChangesSince(ctx, basePath, instantTime)
}

// GetLatestChanges retrieves changes since the last N commits
func (iq *HudiIncrementalQuery) GetLatestChanges(ctx context.Context, basePath string, numCommits int) (*IncrementalQueryResult, error) {
	commits, err := iq.driver.GetCommits(ctx, basePath)
	if err != nil {
		return nil, err
	}

	if len(commits) < numCommits + 1 {
		return nil, fmt.Errorf("not enough commits (need at least %d, have %d)", numCommits+1, len(commits))
	}

	beginInstantTime := commits[len(commits)-numCommits-1].CommitTime
	endInstantTime := commits[len(commits)-1].CommitTime

	return iq.GetChangesInRange(ctx, basePath, beginInstantTime, endInstantTime)
}

// GetPendingCompaction retrieves pending compaction instants
func (iq *HudiIncrementalQuery) GetPendingCompaction(ctx context.Context, basePath string) ([]CompactionInstant, error) {
	// Compaction plan would be stored in Hudi metadata
	// This is a placeholder implementation
	return []CompactionInstant{}, nil
}

// CompactionInstant represents a compaction instant
type CompactionInstant struct {
	InstantTime string
	State       string
}

// GetInserts retrieves only inserts (no updates or deletes)
func (iq *HudiIncrementalQuery) GetInserts(ctx context.Context, basePath, beginInstantTime, endInstantTime string) (*IncrementalQueryResult, error) {
	// Filter for _hoodie_operation = 'INSERT'
	sql := fmt.Sprintf("SELECT * FROM %s WHERE `_hoodie_commit_time` > '%s' AND `_hoodie_commit_time` <= '%s' AND `_hoodie_operation` = 'INSERT'",
		basePath, beginInstantTime, endInstantTime)

	result, err := iq.driver.restClient.QueryTable(ctx, sql)
	if err != nil {
		return nil, err
	}

	return &IncrementalQueryResult{
		Data:    result.Rows,
		Schema:  convertToHudiFields(result.Schema.Fields),
		BeginTime: beginInstantTime,
		EndTime:   endInstantTime,
	}, nil
}

// GetUpdates retrieves only updates
func (iq *HudiIncrementalQuery) GetUpdates(ctx context.Context, basePath, beginInstantTime, endInstantTime string) (*IncrementalQueryResult, error) {
	sql := fmt.Sprintf("SELECT * FROM %s WHERE `_hoodie_commit_time` > '%s' AND `_hoodie_commit_time` <= '%s' AND `_hoodie_operation` = 'UPDATE'",
		basePath, beginInstantTime, endInstantTime)

	result, err := iq.driver.restClient.QueryTable(ctx, sql)
	if err != nil {
		return nil, err
	}

	return &IncrementalQueryResult{
		Data:    result.Rows,
		Schema:  convertToHudiFields(result.Schema.Fields),
		BeginTime: beginInstantTime,
		EndTime:   endInstantTime,
	}, nil
}

// GetDeletes retrieves only deletes
func (iq *HudiIncrementalQuery) GetDeletes(ctx context.Context, basePath, beginInstantTime, endInstantTime string) (*IncrementalQueryResult, error) {
	sql := fmt.Sprintf("SELECT * FROM %s WHERE `_hoodie_commit_time` > '%s' AND `_hoodie_commit_time` <= '%s' AND `_hoodie_operation` = 'DELETE'",
		basePath, beginInstantTime, endInstantTime)

	result, err := iq.driver.restClient.QueryTable(ctx, sql)
	if err != nil {
		return nil, err
	}

	return &IncrementalQueryResult{
		Data:    result.Rows,
		Schema:  convertToHudiFields(result.Schema.Fields),
		BeginTime: beginInstantTime,
		EndTime:   endInstantTime,
	}, nil
}

// GetRecordCountByOperation gets count of records by operation type
func (iq *HudiIncrementalQuery) GetRecordCountByOperation(ctx context.Context, basePath, beginInstantTime, endInstantTime string) (map[string]int64, error) {
	commits, err := iq.driver.GetCommits(ctx, basePath)
	if err != nil {
		return nil, err
	}

	// Filter commits within range
	filteredCommits := filterCommitsByRange(commits, beginInstantTime, endInstantTime)

	// Aggregate by operation
	stats := make(map[string]int64)
	for _, commit := range filteredCommits {
		stats[commit.Operation]++
		if commit.RecordsStats.NumInserts > 0 {
			stats["inserts"] += int64(commit.RecordsStats.NumInserts)
		}
		if commit.RecordsStats.NumUpdates > 0 {
			stats["updates"] += int64(commit.RecordsStats.NumUpdates)
		}
		if commit.RecordsStats.NumDeletes > 0 {
			stats["deletes"] += int64(commit.RecordsStats.NumDeletes)
		}
	}

	return stats, nil
}

// filterCommitsByRange filters commits by instant time range
func filterCommitsByRange(commits []HudiCommit, beginTime, endTime string) []HudiCommit {
	result := make([]HudiCommit, 0)
	for _, commit := range commits {
		if commit.CommitTime > beginTime && commit.CommitTime <= endTime {
			result = append(result, commit)
		}
	}
	return result
}

// GetCDCChanges retrieves change data capture (CDC) changes
func (iq *HudiIncrementalQuery) GetCDCChanges(ctx context.Context, basePath, beginInstantTime, endInstantTime string) (*CDCResult, error) {
	// CDC queries track before/after images
	// This is a placeholder implementation
	return &CDCResult{
		BeforeImages: [][]interface{}{},
		AfterImages:  [][]interface{}{},
	}, nil
}

// CDCResult represents CDC query results
type CDCResult struct {
	BeforeImages [][]interface{}
	AfterImages  [][]interface{}
}

// StreamChanges streams changes as they happen
func (iq *HudiIncrementalQuery) StreamChanges(ctx context.Context, basePath string, callback func(*IncrementalQueryResult) error) error {
	// Streaming would require polling or event-based mechanism
	// This is a placeholder implementation
	return fmt.Errorf("streaming not yet implemented")
}

// GetHudiMetadataFields returns Hudi's metadata fields
func (iq *HudiIncrementalQuery) GetHudiMetadataFields() []HudiMetadataField {
	return []HudiMetadataField{
		{
			Name: "_hoodie_commit_time",
			Type: "string",
			Description: "Commit instant time",
		},
		{
			Name: "_hoodie_commit_seqno",
			Type: "string",
			Description: "Commit sequence number",
		},
		{
			Name: "_hoodie_record_key",
			Type: "string",
			Description: "Record key",
		},
		{
			Name: "_hoodie_partition_path",
			Type: "string",
			Description: "Partition path",
		},
		{
			Name: "_hoodie_file_name",
			Type: "string",
			Description: "File name",
		},
		{
			Name: "_hoodie_operation",
			Type: "string",
			Description: "Operation type (INSERT/UPDATE/DELETE)",
		},
	}
}

// HudiMetadataField represents a Hudi metadata field
type HudiMetadataField struct {
	Name        string
	Type        string
	Description string
}

// ValidateIncrementalQueryRequest validates an incremental query request
func (iq *HudiIncrementalQuery) ValidateIncrementalQueryRequest(req *IncrementalQueryRequest) error {
	if req.BasePath == "" {
		return fmt.Errorf("basePath is required")
	}
	if req.BeginInstantTime == "" {
		return fmt.Errorf("beginInstantTime is required")
	}
	if req.EndInstantTime == "" {
		return fmt.Errorf("endInstantTime is required")
	}

	// Validate instant time format
	if _, err := iq.driver.parser.ParseInstantTime(req.BeginInstantTime); err != nil {
		return fmt.Errorf("invalid beginInstantTime format: %w", err)
	}
	if _, err := iq.driver.parser.ParseInstantTime(req.EndInstantTime); err != nil {
		return fmt.Errorf("invalid endInstantTime format: %w", err)
	}

	return nil
}

// Helper functions

func convertToHudiFields(fields []HudiResultField) []HudiField {
	result := make([]HudiField, len(fields))
	for i, field := range fields {
		result[i] = HudiField{
			Name: field.Name,
			Type: field.Type,
		}
	}
	return result
}

func joinFields(fields []string) string {
	result := ""
	for i, field := range fields {
		if i > 0 {
			result += ", "
		}
		result += field
	}
	return result
}
