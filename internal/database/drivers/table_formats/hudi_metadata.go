package table_formats

import (
	"fmt"
	"time"

	"nexus-gateway/internal/database/metadata"
)

// HudiMetadataParser handles Hudi metadata parsing
type HudiMetadataParser struct{}

// NewHudiMetadataParser creates a new Hudi metadata parser
func NewHudiMetadataParser() *HudiMetadataParser {
	return &HudiMetadataParser{}
}

// ParseTableMetadata parses Hudi table metadata into standard schema
func (p *HudiMetadataParser) ParseTableMetadata(hudiMeta *HudiTableMetadata, tableName string) (*metadata.DataSourceSchema, error) {
	schema := &metadata.DataSourceSchema{
		Tables: make(map[string]*metadata.TableSchema),
	}

	tableSchema := &metadata.TableSchema{
		Name:       tableName,
		Type:       "TABLE",
		Columns:    make([]metadata.ColumnSchema, 0),
		Properties: make(map[string]interface{}),
	}

	// Parse fields
	for _, field := range hudiMeta.RecordFields {
		column := metadata.ColumnSchema{
			Name:     field.Name,
			Type:     ConvertHudiTypeToStandardType(field.Type),
			Nullable: field.Nullable,
		}
		tableSchema.Columns = append(tableSchema.Columns, column)
	}

	// Add Hudi-specific properties
	tableSchema.Properties["table-type"] = ParseHudiTableType(hudiMeta.TableType)
	tableSchema.Properties["key-field"] = hudiMeta.KeyField
	tableSchema.Properties["partition-fields"] = hudiMeta.PartitionFields
	tableSchema.Properties["hudi-version"] = hudiMeta.HudiVersion

	for k, v := range hudiMeta.Properties {
		tableSchema.Properties[k] = v
	}

	schema.Tables[tableName] = tableSchema

	return schema, nil
}

// GetCommitAtTime retrieves the commit for a specific timestamp
func (p *HudiMetadataParser) GetCommitAtTime(commits []HudiCommit, timestamp time.Time) (*HudiCommit, error) {
	targetMs := timestamp.UnixMilli()

	// Find the commit at or before the target timestamp
	var bestCommit *HudiCommit
	for i := range commits {
		commit := &commits[i]
		if commit.TimestampMs <= targetMs {
			if bestCommit == nil || commit.TimestampMs > bestCommit.TimestampMs {
				bestCommit = commit
			}
		}
	}

	if bestCommit == nil {
		return nil, fmt.Errorf("no commit found for timestamp: %v", timestamp)
	}

	return bestCommit, nil
}

// GetLatestCommit retrieves the latest commit
func (p *HudiMetadataParser) GetLatestCommit(commits []HudiCommit) (*HudiCommit, error) {
	if len(commits) == 0 {
		return nil, fmt.Errorf("no commits available")
	}

	// Commits are typically ordered by timestamp
	latest := commits[len(commits)-1]
	return &latest, nil
}

// ParseInstantTime parses Hudi instant time string
func (p *HudiMetadataParser) ParseInstantTime(instantTime string) (time.Time, error) {
	// Hudi instant time format: yyyyMMddHHmmssSSS
	// Example: 20240101153045123
	layout := "20060102150405.000"
	return time.Parse(layout, instantTime[:15]+"."+instantTime[15:])
}

// GetTableType determines the table type
func (p *HudiMetadataParser) GetTableType(metadata *HudiTableMetadata) string {
	return ParseHudiTableType(metadata.TableType)
}

// IsCopyOnWrite checks if table is Copy-On-Write
func (p *HudiMetadataParser) IsCopyOnWrite(metadata *HudiTableMetadata) bool {
	return metadata.TableType == "COPY_ON_WRITE"
}

// IsMergeOnRead checks if table is Merge-On-Read
func (p *HudiMetadataParser) IsMergeOnRead(metadata *HudiTableMetadata) bool {
	return metadata.TableType == "MERGE_ON_READ"
}

// GetPartitionSpec returns partition specification
func (p *HudiMetadataParser) GetPartitionSpec(metadata *HudiTableMetadata) []string {
	return metadata.PartitionFields
}

// CalculateRecordStats calculates record statistics from commits
func (p *HudiMetadataParser) CalculateRecordStats(commits []HudiCommit) *HudiRecordStats {
	totalInserts := 0
	totalUpdates := 0
	totalDeletes := 0

	for _, commit := range commits {
		if commit.RecordsStats.NumInserts > 0 {
			totalInserts += commit.RecordsStats.NumInserts
		}
		if commit.RecordsStats.NumUpdates > 0 {
			totalUpdates += commit.RecordsStats.NumUpdates
		}
		if commit.RecordsStats.NumDeletes > 0 {
			totalDeletes += commit.RecordsStats.NumDeletes
		}
	}

	return &HudiRecordStats{
		TotalInserts: totalInserts,
		TotalUpdates: totalUpdates,
		TotalDeletes: totalDeletes,
		TotalRecords: totalInserts + totalUpdates - totalDeletes,
	}
}

// HudiRecordStats contains aggregated record statistics
type HudiRecordStats struct {
	TotalInserts int `json:"totalInserts"`
	TotalUpdates int `json:"totalUpdates"`
	TotalDeletes int `json:"totalDeletes"`
	TotalRecords int `json:"totalRecords"`
}

// GetOperationTimeline creates a timeline of operations
func (p *HudiMetadataParser) GetOperationTimeline(commits []HudiCommit) []OperationEvent {
	timeline := make([]OperationEvent, len(commits))
	for i, commit := range commits {
		timestamp, _ := p.ParseInstantTime(commit.CommitTime)
		timeline[i] = OperationEvent{
			Timestamp:  timestamp,
			Operation:  commit.Operation,
			ActionType: commit.ActionType,
			State:      commit.State,
		}
	}
	return timeline
}

// OperationEvent represents an operation event
type OperationEvent struct {
	Timestamp  time.Time
	Operation  string
	ActionType string
	State      string
}

// GetCommitsByOperation filters commits by operation type
func (p *HudiMetadataParser) GetCommitsByOperation(commits []HudiCommit, operation string) []HudiCommit {
	result := make([]HudiCommit, 0)
	for _, commit := range commits {
		if commit.Operation == operation {
			result = append(result, commit)
		}
	}
	return result
}

// EstimateRowCount estimates row count based on commits
func (p *HudiMetadataParser) EstimateRowCount(commits []HudiCommit) int64 {
	stats := p.CalculateRecordStats(commits)
	return int64(stats.TotalRecords)
}

// GetPartitionStats calculates statistics per partition
func (p *HudiMetadataParser) GetPartitionStats(partitions []HudiPartition) *PartitionStats {
	stats := &PartitionStats{
		PartitionCount: len(partitions),
		TotalFiles:     0,
		TotalSizeBytes: 0,
	}

	for _, partition := range partitions {
		stats.TotalFiles += partition.FileCount
		stats.TotalSizeBytes += partition.SizeBytes
	}

	return stats
}

// PartitionStats contains partition-level statistics
type PartitionStats struct {
	PartitionCount int
	TotalFiles     int
	TotalSizeBytes int64
}

// ValidateHudiTableConfig validates Hudi table configuration
func (p *HudiMetadataParser) ValidateHudiTableConfig(config map[string]string) error {
	// Validate required Hudi properties
	// hoodie.table.name should be present
	if _, exists := config["hoodie.table.name"]; !exists {
		return fmt.Errorf("hoodie.table.name is required")
	}

	// Validate table type
	if tableType, exists := config["hoodie.table.type"]; exists {
		parsed := ParseHudiTableType(tableType)
		if parsed != "COPY_ON_WRITE" && parsed != "MERGE_ON_READ" {
			return fmt.Errorf("invalid table type: %s", tableType)
		}
	}

	return nil
}

// GetCompressionCodec returns the compression codec
func (p *HudiMetadataParser) GetCompressionCodec(metadata *HudiTableMetadata) string {
	if codec, exists := metadata.Properties["hoodie.parquet.compression.codec"]; exists {
		return codec
	}
	return "gzip" // Default
}

// SupportsTimeTravel checks if the table supports time travel
func (p *HudiMetadataParser) SupportsTimeTravel(metadata *HudiTableMetadata) bool {
	// All Hudi tables support time travel
	return true
}

// SupportsIncrementalQuery checks if the table supports incremental queries
func (p *HudiMetadataParser) SupportsIncrementalQuery(metadata *HudiTableMetadata) bool {
	// Both COW and MOR support incremental queries
	return true
}

// GetCleanupPolicy returns the cleanup policy
func (p *HudiMetadataParser) GetCleanupPolicy(metadata *HudiTableMetadata) string {
	if policy, exists := metadata.Properties["hoodie.cleaner.policy"]; exists {
		return policy
	}
	return "KEEP_LATEST_COMMITS" // Default
}

// GetRollbackPolicy returns the rollback policy
func (p *HudiMetadataParser) GetRollbackPolicy(metadata *HudiTableMetadata) string {
	if policy, exists := metadata.Properties["hoodie.rollback.policy"]; exists {
		return policy
	}
	return "LATEST_COMMITS" // Default
}

// BuildTimeTravelQuery builds a query for time travel
func (p *HudiMetadataParser) BuildTimeTravelQuery(tableName string, instantTime string) string {
	// Hudi time travel syntax varies by compute engine
	// For Spark: SELECT * FROM table TIMESTAMP AS OF 'instant'
	return fmt.Sprintf("SELECT * FROM %s TIMESTAMP AS OF '%s'", tableName, instantTime)
}

// FormatInstantTime formats a timestamp as Hudi instant time
func (p *HudiMetadataParser) FormatInstantTime(timestamp time.Time) string {
	// Format: yyyyMMddHHmmssSSS
	return timestamp.Format("20060102150405") + fmt.Sprintf("%03d", timestamp.Nanosecond()/1000000)
}

// GetCommitTimeline returns timeline with commit information
func (p *HudiMetadataParser) GetCommitTimeline(commits []HudiCommit) []CommitTimelineEntry {
	timeline := make([]CommitTimelineEntry, len(commits))
	for i, commit := range commits {
		timestamp, _ := p.ParseInstantTime(commit.CommitTime)
		timeline[i] = CommitTimelineEntry{
			Timestamp: timestamp,
			Version:   commit.CommitTime,
			Operation: commit.Operation,
		}
	}
	return timeline
}

// GetSavepointAtTime retrieves savepoint at a specific time
func (p *HudiMetadataParser) GetSavepointAtTime(savepoints []HudiSavepoint, timestamp time.Time) (*HudiSavepoint, error) {
	targetMs := timestamp.UnixMilli()

	var bestSavepoint *HudiSavepoint
	for i := range savepoints {
		sp := &savepoints[i]
		spTime, err := p.ParseInstantTime(sp.SavepointTime)
		if err != nil {
			continue
		}
		if spTime.UnixMilli() <= targetMs {
			if bestSavepoint == nil || spTime.After(time.UnixMilli(bestSavepoint.TimestampMs)) {
				bestSavepoint = sp
			}
		}
	}

	if bestSavepoint == nil {
		return nil, fmt.Errorf("no savepoint found for timestamp: %v", timestamp)
	}

	return bestSavepoint, nil
}
