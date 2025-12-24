package object_storage

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3SelectHandler handles S3 Select API operations
type S3SelectHandler struct {
	client *S3Client
}

// NewS3SelectHandler creates a new S3 Select handler
func NewS3SelectHandler(client *S3Client) *S3SelectHandler {
	return &S3SelectHandler{
		client: client,
	}
}

// SelectQuery represents an S3 Select query
type SelectQuery struct {
	Key              string
	SQL              string
	InputSerialization  *InputSerialization
	OutputSerialization *OutputSerialization
}

// InputSerialization defines input format
type InputSerialization struct {
	CompressionType string // NONE, GZIP, BZIP2
	CSV             *CSVInput
	JSON            *JSONInput
	Parquet         *ParquetInput
}

// CSVInput defines CSV input format
type CSVInput struct {
	FileHeaderInfo       string // USE, IGNORE, NONE
	RecordDelimiter      string
	FieldDelimiter       string
	QuoteCharacter       string
	QuoteEscapeCharacter string
	Comments             string
}

// JSONInput defines JSON input format
type JSONInput struct {
	Type string // DOCUMENT, LINES
}

// ParquetInput defines Parquet input format
type ParquetInput struct{}

// OutputSerialization defines output format
type OutputSerialization struct {
	CSV  *CSVOutput
	JSON *JSONOutput
}

// CSVOutput defines CSV output format
type CSVOutput struct {
	QuoteFields          string // ASNEEDED, ALWAYS
	RecordDelimiter      string
	FieldDelimiter       string
	QuoteCharacter       string
	QuoteEscapeCharacter string
}

// JSONOutput defines JSON output format
type JSONOutput struct {
	RecordDelimiter string
}

// SelectResult represents the result of an S3 Select query
type SelectResult struct {
	Records   []map[string]interface{}
	Columns   []string
	Schema    map[string]string // Column name -> type
	BytesScanned int64
	BytesProcessed int64
	BytesReturned int64
}

// Query executes an S3 Select query
func (h *S3SelectHandler) Query(ctx context.Context, query *SelectQuery) (*SelectResult, error) {
	input := &s3.SelectObjectContentInput{
		Bucket:         aws.String(h.client.config.Bucket),
		Key:            aws.String(query.Key),
		Expression:     aws.String(query.SQL),
		ExpressionType: types.ExpressionTypeSql,
		InputSerialization: h.buildInputSerialization(query.InputSerialization),
		OutputSerialization: h.buildOutputSerialization(query.OutputSerialization),
	}

	// Execute query
	result, err := h.client.client.SelectObjectContent(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to execute select query: %w", err)
	}
	defer result.GetStream().Close()

	// Parse response
	selectResult := &SelectResult{
		Records: make([]map[string]interface{}, 0),
	}

	// Read response from event stream
	eventStream := result.GetStream()
	for {
		event, err := eventStream.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("failed to read event: %w", err)
		}

		switch v := event.(type) {
		case *types.SelectObjectContentEventStreamMemberRecords:
			// Parse records
			records, err := h.parseRecords(v.Value.Payload)
			if err != nil {
				return nil, fmt.Errorf("failed to parse records: %w", err)
			}
			selectResult.Records = append(selectResult.Records, records...)

		case *types.SelectObjectContentEventStreamMemberStats:
			// Parse statistics
			stats := v.Value
			selectResult.BytesScanned = aws.ToInt64(stats.BytesScanned)
			selectResult.BytesProcessed = aws.ToInt64(stats.BytesProcessed)
			selectResult.BytesReturned = aws.ToInt64(stats.BytesReturned)

		case *types.SelectObjectContentEventStreamMemberProgress:
			// Progress event - can be used for monitoring

		case *types.SelectObjectContentEventStreamMemberCont:
			// Continuation event - more data coming

		case *types.SelectObjectContentEventStreamMemberEnd:
			// End event - query complete
		}
	}

	// Extract schema and columns from first record
	if len(selectResult.Records) > 0 {
		firstRecord := selectResult.Records[0]
		selectResult.Columns = make([]string, 0, len(firstRecord))
		for col := range firstRecord {
			selectResult.Columns = append(selectResult.Columns, col)
		}
		// Type detection would be done here
		selectResult.Schema = h.inferSchema(selectResult.Records)
	}

	return selectResult, nil
}

// buildInputSerialization builds AWS input serialization from our format
func (h *S3SelectHandler) buildInputSerialization(input *InputSerialization) *types.InputSerialization {
	awsInput := &types.InputSerialization{
		CompressionType: types.CompressionType(input.CompressionType),
	}

	if input.CSV != nil {
		awsInput.CSV = &types.CSVInput{
			FileHeaderInfo:       types.FileHeaderInfo(input.CSV.FileHeaderInfo),
			RecordDelimiter:      aws.String(input.CSV.RecordDelimiter),
			FieldDelimiter:       aws.String(input.CSV.FieldDelimiter),
			QuoteCharacter:       aws.String(input.CSV.QuoteCharacter),
			QuoteEscapeCharacter: aws.String(input.CSV.QuoteEscapeCharacter),
			Comments:             aws.String(input.CSV.Comments),
		}
	}

	if input.JSON != nil {
		awsInput.JSON = &types.JSONInput{
			Type: types.JSONType(input.JSON.Type),
		}
	}

	if input.Parquet != nil {
		awsInput.Parquet = &types.ParquetInput{}
	}

	return awsInput
}

// buildOutputSerialization builds AWS output serialization from our format
func (h *S3SelectHandler) buildOutputSerialization(output *OutputSerialization) *types.OutputSerialization {
	awsOutput := &types.OutputSerialization{}

	if output.CSV != nil {
		awsOutput.CSV = &types.CSVOutput{
			QuoteFields:          types.QuoteFields(output.CSV.QuoteFields),
			RecordDelimiter:      aws.String(output.CSV.RecordDelimiter),
			FieldDelimiter:       aws.String(output.CSV.FieldDelimiter),
			QuoteCharacter:       aws.String(output.CSV.QuoteCharacter),
			QuoteEscapeCharacter: aws.String(output.CSV.QuoteEscapeCharacter),
		}
	}

	if output.JSON != nil {
		awsOutput.JSON = &types.JSONOutput{
			RecordDelimiter: aws.String(output.JSON.RecordDelimiter),
		}
	}

	return awsOutput
}

// parseRecords parses records from S3 Select response payload
func (h *S3SelectHandler) parseRecords(payload []byte) ([]map[string]interface{}, error) {
	// S3 Select returns records in JSON or CSV format
	// We need to determine the format and parse accordingly

	// Try JSON first
	if bytes.HasPrefix(payload, []byte("[")) {
		return h.parseJSONRecords(payload)
	}

	// Fall back to CSV
	return h.parseCSVRecords(payload)
}

// parseJSONRecords parses JSON records
func (h *S3SelectHandler) parseJSONRecords(payload []byte) ([]map[string]interface{}, error) {
	var records []map[string]interface{}
	err := json.Unmarshal(payload, &records)
	if err != nil {
		return nil, fmt.Errorf("failed to parse JSON records: %w", err)
	}
	return records, nil
}

// parseCSVRecords parses CSV records
func (h *S3SelectHandler) parseCSVRecords(payload []byte) ([]map[string]interface{}, error) {
	reader := csv.NewReader(bytes.NewReader(payload))

	// Read header
	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("failed to read CSV header: %w", err)
	}

	// Read records
	var records []map[string]interface{}
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read CSV row: %w", err)
		}

		record := make(map[string]interface{})
		for i, val := range row {
			if i < len(headers) {
				record[headers[i]] = val
			}
		}
		records = append(records, record)
	}

	return records, nil
}

// inferSchema infers schema from records
func (h *S3SelectHandler) inferSchema(records []map[string]interface{}) map[string]string {
	schema := make(map[string]string)

	if len(records) == 0 {
		return schema
	}

	for col, val := range records[0] {
		schema[col] = h.inferType(val)
	}

	return schema
}

// inferType infers type from value
func (h *S3SelectHandler) inferType(value interface{}) string {
	switch value.(type) {
	case int, int32, int64:
		return "int"
	case float32, float64:
		return "float"
	case bool:
		return "boolean"
	case string:
		return "string"
	case []interface{}:
		return "array"
	case map[string]interface{}:
		return "object"
	default:
		return "string"
	}
}

// QueryCSV executes S3 Select on CSV file
func (h *S3SelectHandler) QueryCSV(ctx context.Context, key, sql string, headerPresent bool, delimiter string) (*SelectResult, error) {
	query := &SelectQuery{
		Key: key,
		SQL: sql,
		InputSerialization: &InputSerialization{
			CSV: &CSVInput{
				FileHeaderInfo:  map[bool]string{true: "USE", false: "NONE"}[headerPresent],
				FieldDelimiter:   delimiter,
				RecordDelimiter:  "\n",
				QuoteCharacter:   "\"",
				QuoteEscapeCharacter: "\"",
			},
		},
		OutputSerialization: &OutputSerialization{
			JSON: &JSONOutput{
				RecordDelimiter: "\n",
			},
		},
	}

	return h.Query(ctx, query)
}

// QueryJSON executes S3 Select on JSON file
func (h *S3SelectHandler) QueryJSON(ctx context.Context, key, sql string, jsonType string) (*SelectResult, error) {
	query := &SelectQuery{
		Key: key,
		SQL: sql,
		InputSerialization: &InputSerialization{
			JSON: &JSONInput{
				Type: jsonType, // "DOCUMENT" or "LINES"
			},
		},
		OutputSerialization: &OutputSerialization{
			JSON: &JSONOutput{
				RecordDelimiter: "\n",
			},
		},
	}

	return h.Query(ctx, query)
}

// QueryParquet executes S3 Select on Parquet file
func (h *S3SelectHandler) QueryParquet(ctx context.Context, key, sql string) (*SelectResult, error) {
	query := &SelectQuery{
		Key: key,
		SQL: sql,
		InputSerialization: &InputSerialization{
			Parquet: &ParquetInput{},
		},
		OutputSerialization: &OutputSerialization{
			JSON: &JSONOutput{
				RecordDelimiter: "\n",
			},
		},
	}

	return h.Query(ctx, query)
}

// QueryGZIP executes S3 Select on GZIP-compressed file
func (h *S3SelectHandler) QueryGZIP(ctx context.Context, key, sql string) (*SelectResult, error) {
	query := &SelectQuery{
		Key: key,
		SQL: sql,
		InputSerialization: &InputSerialization{
			CompressionType: "GZIP",
			CSV: &CSVInput{
				FileHeaderInfo:  "USE",
				FieldDelimiter:  ",",
				RecordDelimiter: "\n",
			},
		},
		OutputSerialization: &OutputSerialization{
			JSON: &JSONOutput{
				RecordDelimiter: "\n",
			},
		},
	}

	return h.Query(ctx, query)
}

// GetCompressionType detects compression type from file extension
func GetCompressionType(key string) string {
	if len(key) > 3 && key[len(key)-3:] == ".gz" {
		return "GZIP"
	}
	if len(key) > 4 && key[len(key)-4:] == ".bz2" {
		return "BZIP2"
	}
	return "NONE"
}

// BuildCSVQuery builds S3 Select SQL query for CSV files
func BuildCSVQuery(key, whereClause string, columns []string) string {
	sql := "SELECT "
	if len(columns) > 0 {
		for i, col := range columns {
			if i > 0 {
				sql += ", "
			}
			sql += col
		}
	} else {
		sql += "*"
	}

	sql += fmt.Sprintf(" FROM s3object%s", whereClause)

	return sql
}

// BuildJSONQuery builds S3 Select SQL query for JSON files
func BuildJSONQuery(key, whereClause string, columns []string) string {
	sql := "SELECT "
	if len(columns) > 0 {
		for i, col := range columns {
			if i > 0 {
				sql += ", "
			}
			sql += col
		}
	} else {
		sql += "*"
	}

	sql += fmt.Sprintf(" FROM s3object%s", whereClause)

	return sql
}

// StreamingSelectResult represents a streaming select result
type StreamingSelectResult struct {
	RecordChan chan map[string]interface{}
	ErrorChan  chan error
	Done       chan struct{}
}

// QueryStreaming executes a streaming S3 Select query
func (h *S3SelectHandler) QueryStreaming(ctx context.Context, query *SelectQuery) (*StreamingSelectResult, error) {
	input := &s3.SelectObjectContentInput{
		Bucket:         aws.String(h.client.config.Bucket),
		Key:            aws.String(query.Key),
		Expression:     aws.String(query.SQL),
		ExpressionType: types.ExpressionTypeSql,
		InputSerialization: h.buildInputSerialization(query.InputSerialization),
		OutputSerialization: h.buildOutputSerialization(query.OutputSerialization),
	}

	result, err := h.client.client.SelectObjectContent(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("failed to execute select query: %w", err)
	}

	streamResult := &StreamingSelectResult{
		RecordChan: make(chan map[string]interface{}, 100),
		ErrorChan:  make(chan error, 1),
		Done:       make(chan struct{}),
	}

	// Start goroutine to stream records
	go h.streamRecords(result.GetStream(), streamResult)

	return streamResult, nil
}

// streamRecords streams records from event stream
func (h *S3SelectHandler) streamRecords(eventStream *types.SelectObjectContentEventStream, result *StreamingSelectResult) {
	defer close(result.RecordChan)
	defer close(result.ErrorChan)

	for {
		event, err := eventStream.Read()
		if err != nil {
			if err == io.EOF {
				return
			}
			result.ErrorChan <- fmt.Errorf("failed to read event: %w", err)
			return
		}

		switch v := event.(type) {
		case *types.SelectObjectContentEventStreamMemberRecords:
			records, err := h.parseRecords(v.Value.Payload)
			if err != nil {
				result.ErrorChan <- err
				return
			}

			for _, record := range records {
				select {
				case result.RecordChan <- record:
				case <-result.Done:
					return
				}
			}

		case *types.SelectObjectContentEventStreamMemberEnd:
			return
		}
	}
}

// Close closes the streaming result
func (r *StreamingSelectResult) Close() {
	close(r.Done)
}

// SelectWithLimit executes S3 Select with result limit
func (h *S3SelectHandler) SelectWithLimit(ctx context.Context, query *SelectQuery, limit int) (*SelectResult, error) {
	// Add LIMIT clause to SQL
	if limit > 0 {
		query.SQL = fmt.Sprintf("%s LIMIT %d", query.SQL, limit)
	}

	return h.Query(ctx, query)
}

// SelectWithPagination executes S3 Select with pagination
func (h *S3SelectHandler) SelectWithPagination(ctx context.Context, query *SelectQuery, offset, limit int) (*SelectResult, error) {
	// Add LIMIT and OFFSET clause to SQL
	sql := query.SQL
	if limit > 0 {
		sql = fmt.Sprintf("%s LIMIT %d", sql, limit)
	}
	if offset > 0 {
		sql = fmt.Sprintf("%s OFFSET %d", sql, offset)
	}
	query.SQL = sql

	return h.Query(ctx, query)
}
