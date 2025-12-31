package object_storage

import (
	"context"
	"fmt"
	"io"

	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/source"
)

// ParquetReader reads Parquet files from object storage
type ParquetReader struct {
	s3Client *S3Client
	config   *ParquetReaderConfig
}

// ParquetReaderConfig holds Parquet reader configuration
type ParquetReaderConfig struct {
	BatchSize int
	Pushdown  bool     // Enable predicate pushdown
	Columns   []string // Specific columns to read (empty = all)
}

// NewParquetReader creates a new Parquet reader
func NewParquetReader(s3Client *S3Client, config *ParquetReaderConfig) *ParquetReader {
	if config == nil {
		config = &ParquetReaderConfig{
			BatchSize: 1000,
			Pushdown:  true,
		}
	}

	return &ParquetReader{
		s3Client: s3Client,
		config:   config,
	}
}

// ParquetFileReader wraps parquet-go reader with S3 integration
type ParquetFileReader struct {
	reader   *reader.ParquetReader
	s3Client *S3Client
	key      string
}

// Read reads Parquet file from S3
func (r *ParquetReader) Read(ctx context.Context, key string) (*ParquetFileReader, error) {
	// Download file or use S3 seekable reader
	s3File, err := NewS3File(r.s3Client, key)
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 file: %w", err)
	}

	// Create Parquet reader
	pfReader, err := reader.NewParquetReader(s3File, nil, int64(r.config.BatchSize))
	if err != nil {
		s3File.Close()
		return nil, fmt.Errorf("failed to create Parquet reader: %w", err)
	}

	// Set specific columns if provided
	if len(r.config.Columns) > 0 {
		pfReader.SetReadKeys(r.config.Columns)
	}

	return &ParquetFileReader{
		reader:   pfReader,
		s3Client: r.s3Client,
		key:      key,
	}, nil
}

// ReadNumRows reads a specific number of rows from Parquet file
func (r *ParquetFileReader) ReadNumRows(num int) ([]map[string]interface{}, error) {
	if num <= 0 {
		num = r.reader.GetNumRows()
	}

	res := make([]map[string]interface{}, 0, num)
	for {
		strMap, err := r.reader.ReadByNumber(num)
		if err != nil || len(strMap) == 0 {
			break
		}
		res = append(res, strMap...)
	}

	return res, nil
}

// ReadAll reads all rows from Parquet file
func (r *ParquetFileReader) ReadAll() ([]map[string]interface{}, error) {
	numRows := r.reader.GetNumRows()
	if numRows <= 0 {
		return []map[string]interface{}{}, nil
	}

	return r.ReadNumRows(numRows)
}

// GetSchema returns Parquet file schema
func (r *ParquetFileReader) GetSchema() (*ParquetSchema, error) {
	// Schema information from Parquet reader
	schema := &ParquetSchema{
		Columns: make([]ParquetColumn, 0),
	}

	// Extract schema from parquet file metadata
	if r.reader.PFileHandler != nil {
		pFile := r.reader.PFileHandler

		// Get schema from file metadata
		for i := 0; i < int(pFile.GetNumRows()); i++ {
			// Column schema extraction
			// This is simplified - actual implementation would parse Parquet schema
		}
	}

	return schema, nil
}

// GetNumRows returns the number of rows in the file
func (r *ParquetFileReader) GetNumRows() int {
	return r.reader.GetNumRows()
}

// Close closes the Parquet reader
func (r *ParquetFileReader) Close() error {
	if r.reader != nil {
		r.reader.ReadStop()
	}
	return nil
}

// GetRowGroupCount returns the number of row groups
func (r *ParquetFileReader) GetRowGroupCount() int {
	if r.reader.PFileHandler != nil {
		// Get row group count
		return 1 // Placeholder
	}
	return 0
}

// ParquetSchema represents Parquet file schema
type ParquetSchema struct {
	Columns []ParquetColumn
}

// ParquetColumn represents a Parquet column definition
type ParquetColumn struct {
	Name           string
	Type           string
	RepetitionType string   // REQUIRED, OPTIONAL, REPEATED
	Path           []string // Nested path for complex types
	LogicalType    string   // Logical type annotation (e.g., UTF8, TIMESTAMP, etc.)
}

// ParquetMetadata represents Parquet file metadata
type ParquetMetadata struct {
	Version   int32
	CreatedBy string
	NumRows   int64
	RowGroups []RowGroupMetadata
	Schema    ParquetSchema
}

// RowGroupMetadata represents a row group
type RowGroupMetadata struct {
	NumRows       int64
	TotalByteSize int64
	Columns       []ColumnChunkMetadata
}

// ColumnChunkMetadata represents column chunk metadata
type ColumnChunkMetadata struct {
	FilePath    string
	Offset      int64
	NumValues   int64
	Compression string
	Encodings   []string
	Stats       ColumnStats
	Type        string
}

// ColumnStats represents column statistics
type ColumnStats struct {
	NullCount     int64
	DistinctCount int64
	MinValue      interface{}
	MaxValue      interface{}
}

// GetMetadata retrieves Parquet file metadata
func (r *ParquetReader) GetMetadata(ctx context.Context, key string) (*ParquetMetadata, error) {
	s3File, err := NewS3File(r.s3Client, key)
	if err != nil {
		return nil, fmt.Errorf("failed to create S3 file: %w", err)
	}
	defer s3File.Close()

	// Read Parquet footer to get metadata
	pfReader, err := reader.NewParquetReader(s3File, nil, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to create Parquet reader: %w", err)
	}
	defer pfReader.ReadStop()

	metadata := &ParquetMetadata{
		NumRows: int64(pfReader.GetNumRows()),
	}

	return metadata, nil
}

// S3File implements source.LocalFileReader interface for S3
type S3File struct {
	s3Client *S3Client
	key      string
	size     int64
	position int64
	buffer   []byte
	ctx      context.Context
}

// NewS3File creates a new S3 file reader
func NewS3File(s3Client *S3Client, key string) (*S3File, error) {
	size, err := s3Client.GetObjectSize(context.Background(), key)
	if err != nil {
		return nil, fmt.Errorf("failed to get object size: %w", err)
	}

	return &S3File{
		s3Client: s3Client,
		key:      key,
		size:     size,
		ctx:      context.Background(),
	}, nil
}

// Read implements io.Reader interface
func (f *S3File) Read(p []byte) (n int, err error) {
	// If buffer is empty or exhausted, fetch more data
	if len(f.buffer) == 0 {
		// Calculate how much to read (up to 10MB at a time)
		readSize := int64(len(p))
		if readSize > 10*1024*1024 {
			readSize = 10 * 1024 * 1024
		}

		// Check if we're at EOF
		if f.position >= f.size {
			return 0, io.EOF
		}

		// Fetch range from S3
		end := f.position + readSize - 1
		if end >= f.size {
			end = f.size - 1
		}

		data, err := f.s3Client.GetObjectRange(f.ctx, f.key, f.position, end)
		if err != nil {
			return 0, fmt.Errorf("failed to get object range: %w", err)
		}

		f.buffer = data
	}

	// Copy from buffer to p
	n = copy(p, f.buffer)
	f.buffer = f.buffer[n:]
	f.position += int64(n)

	return n, nil
}

// Seek implements io.Seeker interface
func (f *S3File) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		f.position = offset
	case io.SeekCurrent:
		f.position += offset
	case io.SeekEnd:
		f.position = f.size + offset
	default:
		return 0, fmt.Errorf("invalid whence: %d", whence)
	}

	// Invalidate buffer on seek
	f.buffer = nil

	if f.position < 0 {
		f.position = 0
	} else if f.position > f.size {
		f.position = f.size
	}

	return f.position, nil
}

// Close implements io.Closer interface
func (f *S3File) Close() error {
	f.buffer = nil
	return nil
}

// GetSize returns the file size
func (f *S3File) GetSize() int64 {
	return f.size
}

// GetPosition returns current position
func (f *S3File) GetPosition() int64 {
	return f.position
}

// ParquetRowIterator iterates over rows in a Parquet file
type ParquetRowIterator struct {
	reader *ParquetFileReader
	buffer []map[string]interface{}
	index  int
}

// NewParquetRowIterator creates a new row iterator
func NewParquetRowIterator(reader *ParquetFileReader, batchSize int) *ParquetRowIterator {
	return &ParquetRowIterator{
		reader: reader,
		buffer: make([]map[string]interface{}, 0),
		index:  0,
	}
}

// Next returns the next row or error
func (it *ParquetRowIterator) Next() (map[string]interface{}, error) {
	// If buffer is exhausted, read more
	if it.index >= len(it.buffer) {
		rows, err := it.reader.ReadNumRows(it.reader.reader.GetNumRows())
		if err != nil {
			return nil, err
		}
		it.buffer = rows
		it.index = 0

		// If still no rows, we're done
		if len(it.buffer) == 0 {
			return nil, io.EOF
		}
	}

	row := it.buffer[it.index]
	it.index++

	return row, nil
}

// HasNext checks if there are more rows
func (it *ParquetRowIterator) HasNext() bool {
	return it.index < len(it.buffer) || it.reader.reader.GetNumRows() > 0
}

// Close closes the iterator
func (it *ParquetRowIterator) Close() error {
	return it.reader.Close()
}

// ParquetPredicate represents a predicate for pushdown filtering
type ParquetPredicate struct {
	Column   string
	Operator string // EQ, NEQ, LT, LTE, GT, GTE, IN, IS_NULL
	Value    interface{}
	Values   []interface{} // For IN operator
}

// ParquetFilter represents a filter expression
type ParquetFilter struct {
	Predicates      []ParquetPredicate
	LogicalOperator string // AND, OR
}

// FilterParquetFile reads Parquet file with predicate pushdown
func (r *ParquetReader) FilterParquetFile(ctx context.Context, key string, filter *ParquetFilter) ([]map[string]interface{}, error) {
	// For now, read entire file and filter in-memory
	// Full predicate pushdown would require using Parquet statistics
	reader, err := r.Read(ctx, key)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	allRows, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	// Apply filter
	filtered := make([]map[string]interface{}, 0)
	for _, row := range allRows {
		if r.matchesFilter(row, filter) {
			filtered = append(filtered, row)
		}
	}

	return filtered, nil
}

// matchesFilter checks if a row matches the filter
func (r *ParquetReader) matchesFilter(row map[string]interface{}, filter *ParquetFilter) bool {
	if len(filter.Predicates) == 0 {
		return true
	}

	results := make([]bool, len(filter.Predicates))
	for i, pred := range filter.Predicates {
		results[i] = r.matchesPredicate(row, pred)
	}

	// Apply logical operator
	if filter.LogicalOperator == "OR" {
		for _, result := range results {
			if result {
				return true
			}
		}
		return false
	}
	// Default AND
	for _, result := range results {
		if !result {
			return false
		}
	}
	return true
}

// matchesPredicate checks if a row matches a predicate
func (r *ParquetReader) matchesPredicate(row map[string]interface{}, pred ParquetPredicate) bool {
	val, exists := row[pred.Column]
	if !exists {
		return false
	}

	switch pred.Operator {
	case "EQ":
		return r.compareValues(val, pred.Value) == 0
	case "NEQ":
		return r.compareValues(val, pred.Value) != 0
	case "LT":
		return r.compareValues(val, pred.Value) < 0
	case "LTE":
		return r.compareValues(val, pred.Value) <= 0
	case "GT":
		return r.compareValues(val, pred.Value) > 0
	case "GTE":
		return r.compareValues(val, pred.Value) >= 0
	case "IS_NULL":
		return val == nil
	case "IN":
		for _, v := range pred.Values {
			if r.compareValues(val, v) == 0 {
				return true
			}
		}
		return false
	default:
		return false
	}
}

// compareValues compares two values
func (r *ParquetReader) compareValues(a, b interface{}) int {
	// Simplified comparison - would need proper type handling
	aStr, aOk := a.(string)
	bStr, bOk := b.(string)

	if aOk && bOk {
		if aStr < bStr {
			return -1
		} else if aStr > bStr {
			return 1
		}
		return 0
	}

	// Try numeric comparison
	aFloat, aOk := a.(float64)
	bFloat, bOk := b.(float64)

	if aOk && bOk {
		if aFloat < bFloat {
			return -1
		} else if aFloat > bFloat {
			return 1
		}
		return 0
	}

	return 0
}

// GetColumnNames returns column names from Parquet schema
func (r *ParquetReader) GetColumnNames(ctx context.Context, key string) ([]string, error) {
	reader, err := r.Read(ctx, key)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	// Get schema to extract column names
	// This is a simplified implementation
	if reader.reader.PFileHandler != nil {
		// Extract from schema
		return []string{}, nil
	}

	return []string{}, nil
}

// ReadColumns reads specific columns from Parquet file
func (r *ParquetReader) ReadColumns(ctx context.Context, key string, columns []string) ([]map[string]interface{}, error) {
	config := &ParquetReaderConfig{
		BatchSize: r.config.BatchSize,
		Pushdown:  r.config.Pushdown,
		Columns:   columns,
	}

	readerWithConfig := NewParquetReader(r.s3Client, config)
	fileReader, err := readerWithConfig.Read(ctx, key)
	if err != nil {
		return nil, err
	}
	defer fileReader.Close()

	return fileReader.ReadAll()
}

// EstimateCompressionRatio estimates compression ratio based on metadata
func (r *ParquetReader) EstimateCompressionRatio(ctx context.Context, key string) (float64, error) {
	metadata, err := r.GetMetadata(ctx, key)
	if err != nil {
		return 0, err
	}

	// Calculate compression ratio from metadata
	totalSize := int64(0)
	uncompressedSize := int64(0)

	for _, rg := range metadata.RowGroups {
		for _, col := range rg.Columns {
			totalSize += col.Offset               // Approximation
			uncompressedSize += col.NumValues * 8 // Rough estimate
		}
	}

	if uncompressedSize == 0 {
		return 1.0, nil
	}

	return float64(uncompressedSize) / float64(totalSize), nil
}
