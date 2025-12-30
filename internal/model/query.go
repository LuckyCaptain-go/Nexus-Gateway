package model

import (
	"time"
)

// QueryRequest represents a query execution request (for backward compatibility)
type QueryRequest struct {
	DataSourceID string        `json:"dataSourceId" validate:"required,uuid4"`
	SQL          string        `json:"sql" validate:"required"`
	Parameters   []interface{} `json:"parameters"`
	Limit        int           `json:"limit" validate:"omitempty,min=1,max=10000"`
	Offset       int           `json:"offset" validate:"omitempty,min=0"`
	Timeout      int           `json:"timeout" validate:"omitempty,min=1,max=300"` // timeout in seconds
}

// FetchQueryRequest represents a fetch query request for batch data retrieval
type FetchQueryRequest struct {
	DataSourceID string `json:"dataSourceId" validate:"required,uuid4"`
	SQL          string `json:"sql" validate:"required"`
	Type         int    `json:"type"`                 // 1-同步查询 2-流式查询
	BatchSize    int    `json:"batch_size,omitempty"` // 当前批次数据的预期条数，范围：1-10000，默认10000
	Timeout      int    `json:"timeout,omitempty"`    // 获取第一批数据的预期时间，单位：秒，范围：1-1800，流式查询生效
}

// FetchQueryResponse represents the response for a fetch query
type FetchQueryResponse struct {
	QueryID    string       `json:"queryId"`
	Slug       string       `json:"slug"`
	Token      string       `json:"token"`
	NextURI    string       `json:"nextUri,omitempty"`
	Columns    []ColumnInfo `json:"columns"`
	Entries    [][]string   `json:"entries"`
	TotalCount int          `json:"totalCount"`
}

// ColumnInfo represents column information in the result set
type ColumnInfo struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

// QueryResponse represents the response for a query execution
type QueryResponse struct {
	Columns  []ColumnInfo    `json:"columns"`
	Rows     [][]interface{} `json:"rows"`
	Metadata QueryMetadata   `json:"metadata"`
	Success  bool            `json:"success"`
	Error    *QueryError     `json:"error,omitempty"`
}

// ColumnInfo represents column information in the result set
type ColumnInfo struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

// QueryMetadata contains metadata about the query execution
type QueryMetadata struct {
	RowCount        int       `json:"rowCount"`
	ExecutionTimeMs int64     `json:"executionTimeMs"`
	DataSourceID    string    `json:"dataSourceId"`
	DatabaseType    string    `json:"databaseType"`
	HasMore         bool      `json:"hasMore"`
	Limit           int       `json:"limit"`
	Offset          int       `json:"offset"`
	ExecutedAt      time.Time `json:"executedAt"`
}

// QueryError represents query-specific error information
type QueryError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	SQL     string `json:"sql,omitempty"`
	Details string `json:"details,omitempty"`
}

// StandardizedResult represents a standardized query result across different database types
type StandardizedResult struct {
	Columns []StandardizedColumn `json:"columns"`
	Rows    []StandardizedRow    `json:"rows"`
	Count   int64                `json:"count"`
	HasMore bool                 `json:"hasMore"`
}

// StandardizedColumn represents a standardized column definition
type StandardizedColumn struct {
	Name     string           `json:"name"`
	Type     StandardizedType `json:"type"`
	Nullable bool             `json:"nullable"`
	Original string           `json:"original"` // Original database type
}

// StandardizedRow represents a row of data with standardized types
type StandardizedRow []interface{}

// StandardizedType represents standardized data types across databases
type StandardizedType string

const (
	TypeInteger   StandardizedType = "integer"
	TypeBigInt    StandardizedType = "bigint"
	TypeFloat     StandardizedType = "float"
	TypeDouble    StandardizedType = "double"
	TypeDecimal   StandardizedType = "decimal"
	TypeString    StandardizedType = "string"
	TypeText      StandardizedType = "text"
	TypeBoolean   StandardizedType = "boolean"
	TypeDate      StandardizedType = "date"
	TypeTime      StandardizedType = "time"
	TypeDateTime  StandardizedType = "datetime"
	TypeTimestamp StandardizedType = "timestamp"
	TypeBinary    StandardizedType = "binary"
	TypeJSON      StandardizedType = "json"
	TypeUUID      StandardizedType = "uuid"
	TypeUnknown   StandardizedType = "unknown"
)

// QueryStats represents query execution statistics
type QueryStats struct {
	TotalQueries      int64            `json:"totalQueries"`
	SuccessfulQueries int64            `json:"successfulQueries"`
	FailedQueries     int64            `json:"failedQueries"`
	AvgExecutionTime  float64          `json:"avgExecutionTime"`
	LastQueryTime     time.Time        `json:"lastQueryTime"`
	QueriesByType     map[string]int64 `json:"queriesByType"`
}

// IsValid checks if the QueryRequest is valid
func (qr *QueryRequest) IsValid() bool {
	if qr.DataSourceID == "" || qr.SQL == "" {
		return false
	}
	return true
}

// ApplyDefaults applies default values to the QueryRequest
func (qr *QueryRequest) ApplyDefaults() {
	if qr.Limit <= 0 {
		qr.Limit = 1000 // Default limit
	}
	if qr.Limit > 10000 {
		qr.Limit = 10000 // Max limit
	}
	if qr.Timeout <= 0 {
		qr.Timeout = 30 // Default timeout 30 seconds
	}
}
