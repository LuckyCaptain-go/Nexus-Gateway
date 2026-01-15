package warehouses

import (
	"bytes"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"nexus-gateway/internal/model"
)

// BigQueryTypeMapper handles BigQuery-specific type mappings
type BigQueryTypeMapper struct {
	// Location can affect how certain types are handled
	location string
}

// NewBigQueryTypeMapper creates a new BigQuery type mapper
func NewBigQueryTypeMapper(location string) *BigQueryTypeMapper {
	return &BigQueryTypeMapper{
		location: location,
	}
}

// BigQueryTypeInfo contains detailed type information
type BigQueryTypeInfo struct {
	Name             string
	IsRepeated       bool // Is ARRAY
	IsNullable       bool
	FieldType        *BigQueryTypeInfo // For STRUCT fields
	ArrayElementType *BigQueryTypeInfo // For ARRAY element types
}

// MapBigQueryTypeToStandardType converts BigQuery type to standardized type
func (m *BigQueryTypeMapper) MapBigQueryTypeToStandardType(bqType string, isRepeated bool) model.StandardizedType {
	switch bqType {
	case "STRING":
		if isRepeated {
			return model.TypeArray
		}
		return model.TypeString
	case "BYTES":
		if isRepeated {
			return model.TypeArray
		}
		return model.TypeBinary
	case "INTEGER", "INT64":
		if isRepeated {
			return model.TypeArray
		}
		return model.TypeBigInt
	case "FLOAT", "FLOAT64":
		if isRepeated {
			return model.TypeArray
		}
		return model.TypeFloat
	case "BOOLEAN", "BOOL":
		if isRepeated {
			return model.TypeArray
		}
		return model.TypeBoolean
	case "TIMESTAMP":
		if isRepeated {
			return model.TypeArray
		}
		return model.TypeTimestamp
	case "DATE":
		if isRepeated {
			return model.TypeArray
		}
		return model.TypeDate
	case "TIME":
		if isRepeated {
			return model.TypeArray
		}
		return model.TypeTime
	case "DATETIME":
		if isRepeated {
			return model.TypeArray
		}
		return model.TypeDateTime
	case "NUMERIC", "BIGNUMERIC":
		if isRepeated {
			return model.TypeArray
		}
		return model.TypeDecimal
	case "JSON":
		if isRepeated {
			return model.TypeArray
		}
		return model.TypeJSON
	case "STRUCT", "RECORD":
		if isRepeated {
			return model.TypeArray
		}
		return model.TypeStruct
	case "GEOGRAPHY":
		if isRepeated {
			return model.TypeArray
		}
		return model.TypeGeography
	default:
		return model.TypeString
	}
}

// ConvertBigQueryValue converts BigQuery value to Go native type
func (m *BigQueryTypeMapper) ConvertBigQueryValue(value interface{}, fieldSchema *bigquery.FieldSchema) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	// Handle repeated fields (ARRAY)
	if fieldSchema.Repeated {
		return m.convertArrayValue(value, fieldSchema)
	}

	// Handle STRUCT/RECORD
	if fieldSchema.Type == "STRUCT" || fieldSchema.Type == "RECORD" {
		return m.convertStructValue(value, fieldSchema)
	}

	// Handle simple types
	switch fieldSchema.Type {
	case "STRING":
		return m.convertString(value)
	case "BYTES":
		return m.convertBytes(value)
	case "INTEGER", "INT64":
		return m.convertInt64(value)
	case "FLOAT", "FLOAT64":
		return m.convertFloat64(value)
	case "BOOLEAN", "BOOL":
		return m.convertBoolean(value)
	case "TIMESTAMP":
		return m.convertTimestamp(value)
	case "DATE":
		return m.convertDate(value)
	case "TIME":
		return m.convertTime(value)
	case "DATETIME":
		return m.convertDateTime(value)
	case "NUMERIC", "BIGNUMERIC":
		return m.convertNumeric(value)
	case "JSON":
		return m.convertJSON(value)
	case "GEOGRAPHY":
		return m.convertGeography(value)
	default:
		return value, nil
	}
}

// convertArrayValue handles ARRAY type values
func (m *BigQueryTypeMapper) convertArrayValue(value interface{}, fieldSchema *bigquery.FieldSchema) ([]interface{}, error) {
	// BigQuery returns arrays as []interface{}
	arr, ok := value.([]interface{})
	if !ok {
		// Handle JSON array representation
		return m.parseJSONArray(value)
	}

	result := make([]interface{}, 0, len(arr))
	for _, item := range arr {
		// Create a temporary field schema for array element
		elementSchema := &bigquery.FieldSchema{
			Type:   fieldSchema.Type,
			Schema: fieldSchema.Schema,
		}
		converted, err := m.ConvertBigQueryValue(item, elementSchema)
		if err != nil {
			return nil, err
		}
		result = append(result, converted)
	}

	return result, nil
}

// parseJSONArray handles JSON-encoded array strings
func (m *BigQueryTypeMapper) parseJSONArray(value interface{}) ([]interface{}, error) {
	var arr []interface{}

	switch v := value.(type) {
	case string:
		if err := json.Unmarshal([]byte(v), &arr); err != nil {
			return nil, fmt.Errorf("failed to parse JSON array: %w", err)
		}
		return arr, nil
	case []byte:
		if err := json.Unmarshal(v, &arr); err != nil {
			return nil, fmt.Errorf("failed to parse JSON array: %w", err)
		}
		return arr, nil
	default:
		return nil, fmt.Errorf("unsupported array type: %T", value)
	}
}

// convertStructValue handles STRUCT/RECORD type values
func (m *BigQueryTypeMapper) convertStructValue(value interface{}, fieldSchema *bigquery.FieldSchema) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	// BigQuery returns structs as []interface{} with values in field order
	valList, ok := value.([]interface{})
	if !ok {
		// Handle JSON object representation
		return m.parseJSONStruct(value)
	}

	if len(valList) != len(fieldSchema.Schema) {
		return nil, fmt.Errorf("struct field count mismatch: expected %d, got %d",
			len(fieldSchema.Schema), len(valList))
	}

	for i, field := range fieldSchema.Schema {
		converted, err := m.ConvertBigQueryValue(valList[i], field)
		if err != nil {
			return nil, fmt.Errorf("failed to convert field %s: %w", field.Name, err)
		}
		result[field.Name] = converted
	}

	return result, nil
}

// parseJSONStruct handles JSON-encoded struct strings
func (m *BigQueryTypeMapper) parseJSONStruct(value interface{}) (map[string]interface{}, error) {
	var result map[string]interface{}

	switch v := value.(type) {
	case string:
		if err := json.Unmarshal([]byte(v), &result); err != nil {
			return nil, fmt.Errorf("failed to parse JSON struct: %w", err)
		}
		return result, nil
	case []byte:
		if err := json.Unmarshal(v, &result); err != nil {
			return nil, fmt.Errorf("failed to parse JSON struct: %w", err)
		}
		return result, nil
	default:
		return nil, fmt.Errorf("unsupported struct type: %T", value)
	}
}

// Type conversion helpers for simple types
func (m *BigQueryTypeMapper) convertString(value interface{}) (string, error) {
	if s, ok := value.(string); ok {
		return s, nil
	}
	return fmt.Sprintf("%v", value), nil
}

func (m *BigQueryTypeMapper) convertBytes(value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		return nil, fmt.Errorf("unsupported bytes type: %T", value)
	}
}

func (m *BigQueryTypeMapper) convertInt64(value interface{}) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case string:
		var i int64
		fmt.Sscanf(v, "%d", &i)
		return i, nil
	default:
		return 0, fmt.Errorf("unsupported int64 type: %T", value)
	}
}

func (m *BigQueryTypeMapper) convertFloat64(value interface{}) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case int64:
		return float64(v), nil
	case int:
		return float64(v), nil
	case string:
		var f float64
		fmt.Sscanf(v, "%f", &f)
		return f, nil
	default:
		return 0, fmt.Errorf("unsupported float64 type: %T", value)
	}
}

func (m *BigQueryTypeMapper) convertBoolean(value interface{}) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case string:
		return v == "true" || v == "t" || v == "1", nil
	case int64:
		return v != 0, nil
	default:
		return false, fmt.Errorf("unsupported boolean type: %T", value)
	}
}

func (m *BigQueryTypeMapper) convertTimestamp(value interface{}) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case string:
		// BigQuery timestamp format: RFC3339 with nanoseconds
		return time.Parse(time.RFC3339Nano, v)
	default:
		return time.Time{}, fmt.Errorf("unsupported timestamp type: %T", value)
	}
}

func (m *BigQueryTypeMapper) convertDate(value interface{}) (string, error) {
	// BigQuery DATE is returned as string in YYYY-MM-DD format
	if s, ok := value.(string); ok {
		return s, nil
	}
	return fmt.Sprintf("%v", value), nil
}

func (m *BigQueryTypeMapper) convertTime(value interface{}) (string, error) {
	// BigQuery TIME is returned as string in HH:MM:SS or HH:MM:SS.SSSSSS format
	if s, ok := value.(string); ok {
		return s, nil
	}
	return fmt.Sprintf("%v", value), nil
}

func (m *BigQueryTypeMapper) convertDateTime(value interface{}) (string, error) {
	// BigQuery DATETIME is returned as string in YYYY-MM-DD HH:MM:SS format
	if s, ok := value.(string); ok {
		return s, nil
	}
	return fmt.Sprintf("%v", value), nil
}

func (m *BigQueryTypeMapper) convertNumeric(value interface{}) (string, error) {
	// BigQuery NUMERIC is high-precision decimal
	// Return as string to preserve precision
	if s, ok := value.(string); ok {
		return s, nil
	}
	if f, ok := value.(float64); ok {
		return fmt.Sprintf("%.2f", f), nil
	}
	return fmt.Sprintf("%v", value), nil
}

func (m *BigQueryTypeMapper) convertJSON(value interface{}) (interface{}, error) {
	// BigQuery JSON type - parse and return as-is
	switch v := value.(type) {
	case string:
		var result interface{}
		if err := json.Unmarshal([]byte(v), &result); err != nil {
			return nil, fmt.Errorf("failed to parse JSON: %w", err)
		}
		return result, nil
	case []byte:
		var result interface{}
		if err := json.Unmarshal(v, &result); err != nil {
			return nil, fmt.Errorf("failed to parse JSON: %w", err)
		}
		return result, nil
	default:
		return v, nil
	}
}

func (m *BigQueryTypeMapper) convertGeography(value interface{}) (string, error) {
	// BigQuery GEOGRAPHY type - return WKT representation
	if s, ok := value.(string); ok {
		return s, nil
	}
	return fmt.Sprintf("%v", value), nil
}

// GetFieldSchema extracts field schema information
func (m *BigQueryTypeMapper) GetFieldSchema(field *bigquery.FieldSchema) *BigQueryTypeInfo {
	info := &BigQueryTypeInfo{
		Name:       field.Name,
		IsRepeated: field.Repeated,
		IsNullable: !field.Required,
	}

	typeStr := string(field.Type)
	if typeStr == "STRUCT" || typeStr == "RECORD" {
		info.FieldType = &BigQueryTypeInfo{
			Name: typeStr,
		}
	}

	if field.Repeated && field.Schema != nil {
		info.ArrayElementType = &BigQueryTypeInfo{
			Name: typeStr,
		}
	}

	return info
}

// ConvertRow converts a BigQuery row to map[string]interface{}
func (m *BigQueryTypeMapper) ConvertRow(row []interface{}, schema bigquery.Schema) (map[string]interface{}, error) {
	result := make(map[string]interface{})

	for i, field := range schema {
		if i >= len(row) {
			break
		}

		converted, err := m.ConvertBigQueryValue(row[i], field)
		if err != nil {
			return nil, fmt.Errorf("failed to convert field %s: %w", field.Name, err)
		}
		result[field.Name] = converted
	}

	return result, nil
}

// ConvertRows converts multiple BigQuery rows
func (m *BigQueryTypeMapper) ConvertRows(rows [][]interface{}, schema bigquery.Schema) ([]map[string]interface{}, error) {
	result := make([]map[string]interface{}, len(rows))

	for i, row := range rows {
		converted, err := m.ConvertRow(row, schema)
		if err != nil {
			return nil, fmt.Errorf("failed to convert row %d: %w", i, err)
		}
		result[i] = converted
	}

	return result, nil
}

// FormatBigQueryValue formats a Go value for BigQuery
func (m *BigQueryTypeMapper) FormatBigQueryValue(value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	switch v := value.(type) {
	case time.Time:
		return v.Format(time.RFC3339Nano), nil
	case []byte:
		return string(v), nil
	case map[string]interface{}:
		// Convert struct to JSON
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal struct: %w", err)
		}
		return string(jsonBytes), nil
	case []interface{}:
		// Convert array to JSON
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal array: %w", err)
		}
		return string(jsonBytes), nil
	default:
		return v, nil
	}
}

// BigQuerySchemaConverter helps convert between BigQuery and standard schemas
type BigQuerySchemaConverter struct {
	mapper *BigQueryTypeMapper
}

// NewBigQuerySchemaConverter creates a new schema converter
func NewBigQuerySchemaConverter(location string) *BigQuerySchemaConverter {
	return &BigQuerySchemaConverter{
		mapper: NewBigQueryTypeMapper(location),
	}
}

// ConvertToStandardSchema converts BigQuery schema to standard schema
func (c *BigQuerySchemaConverter) ConvertToStandardSchema(bqSchema bigquery.Schema) model.TableSchema {
	stdSchema := model.TableSchema{
		Columns: make([]model.ColumnInfo, 0, len(bqSchema)),
	}

	for _, field := range bqSchema {
		typeStr := string(field.Type)
		col := model.ColumnInfo{
			Name:     field.Name,
			Type:     c.mapper.MapBigQueryTypeToStandardType(typeStr, field.Repeated),
			Nullable: !field.Required,
		}

		// Handle nested structs
		if typeStr == "STRUCT" || typeStr == "RECORD" {
			col.NestedFields = c.convertStructSchema(field.Schema)
		}

		stdSchema.Columns = append(stdSchema.Columns, col)
	}

	return stdSchema
}

// convertStructSchema converts nested struct schema
func (c *BigQuerySchemaConverter) convertStructSchema(fields bigquery.Schema) []model.ColumnInfo {
	cols := make([]model.ColumnInfo, 0, len(fields))

	for _, field := range fields {
		typeStr := string(field.Type)
		col := model.ColumnInfo{
			Name:     field.Name,
			Type:     c.mapper.MapBigQueryTypeToStandardType(typeStr, field.Repeated),
			Nullable: !field.Required,
		}

		if typeStr == "STRUCT" || typeStr == "RECORD" {
			col.NestedFields = c.convertStructSchema(field.Schema)
		}

		cols = append(cols, col)
	}

	return cols
}

// BuildBigQuerySchema builds BigQuery schema from standard schema
func (c *BigQuerySchemaConverter) BuildBigQuerySchema(stdSchema model.TableSchema) bigquery.Schema {
	bqSchema := make(bigquery.Schema, 0, len(stdSchema.Columns))

	for _, col := range stdSchema.Columns {
		field := &bigquery.FieldSchema{
			Name:     col.Name,
			Type:     bigquery.FieldType(c.convertStandardTypeToBigQuery(col.Type)),
			Required: !col.Nullable,
		}

		if col.NestedFields != nil {
			field.Schema = c.buildNestedSchema(col.NestedFields)
		}

		bqSchema = append(bqSchema, field)
	}

	return bqSchema
}

// convertStandardTypeToBigQuery converts standard type to BigQuery type string
func (c *BigQuerySchemaConverter) convertStandardTypeToBigQuery(stdType model.StandardizedType) string {
	switch stdType {
	case model.TypeString:
		return "STRING"
	case model.TypeBinary:
		return "BYTES"
	case model.TypeBigInt:
		return "INT64"
	case model.TypeFloat:
		return "FLOAT64"
	case model.TypeBoolean:
		return "BOOLEAN"
	case model.TypeTimestamp:
		return "TIMESTAMP"
	case model.TypeDate:
		return "DATE"
	case model.TypeTime:
		return "TIME"
	case model.TypeDateTime:
		return "DATETIME"
	case model.TypeDecimal:
		return "NUMERIC"
	case model.TypeJSON:
		return "JSON"
	case model.TypeStruct:
		return "STRUCT"
	case model.TypeArray:
		return "ARRAY" // Element type depends on context
	case model.TypeGeography:
		return "GEOGRAPHY"
	default:
		return "STRING"
	}
}

// buildNestedSchema builds nested schema for structs
func (c *BigQuerySchemaConverter) buildNestedSchema(cols []model.ColumnInfo) bigquery.Schema {
	schema := make(bigquery.Schema, 0, len(cols))

	for _, col := range cols {
		field := &bigquery.FieldSchema{
			Name:     col.Name,
			Type:     bigquery.FieldType(c.convertStandardTypeToBigQuery(col.Type)),
			Required: !col.Nullable,
		}

		if col.NestedFields != nil {
			field.Schema = c.buildNestedSchema(col.NestedFields)
		}

		schema = append(schema, field)
	}

	return schema
}

// SerializeStruct serializes a struct to JSON for BigQuery STRUCT type
func (c *BigQuerySchemaConverter) SerializeStruct(structMap map[string]interface{}) (string, error) {
	jsonBytes, err := json.Marshal(structMap)
	if err != nil {
		return "", fmt.Errorf("failed to serialize struct: %w", err)
	}

	// Format as BigQuery struct literal
	var buf bytes.Buffer
	buf.WriteString("STRUCT<")
	var fields []map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &fields); err == nil {
		// Handle as array
	}

	buf.WriteString("(")
	// Add struct fields
	buf.WriteString(")")

	return buf.String(), nil
}
