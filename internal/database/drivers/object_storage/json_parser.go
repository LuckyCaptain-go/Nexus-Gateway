package object_storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// JSONParser handles JSON file parsing with nested structure support
type JSONParser struct {
	config *JSONParserConfig
}

// JSONParserConfig holds JSON parser configuration
type JSONParserConfig struct {
	DetectTypes   bool   // Auto-detect types from values
	FlattenNested bool   // Flatten nested structures
	Separator     string // Separator for flattened keys (default: "_")
}

// NewJSONParser creates a new JSON parser
func NewJSONParser(config *JSONParserConfig) *JSONParser {
	if config == nil {
		config = &JSONParserConfig{
			DetectTypes:   true,
			FlattenNested: false,
			Separator:     "_",
		}
	}

	return &JSONParser{
		config: config,
	}
}

// JSONSchema represents detected JSON schema
type JSONSchema struct {
	Fields     []JSONField
	IsArray    bool
	ArrayDepth int
}

// JSONField represents a JSON field definition
type JSONField struct {
	Name             string
	Path             string // Dot-notation path for nested fields
	Type             string
	Nullable         bool
	NestedFields     []JSONField // For object types
	ArrayElementType *JSONField  // For array types
}

// ParsedJSON represents parsed JSON data with schema
type ParsedJSON struct {
	Data   []map[string]interface{}
	Schema JSONSchema
}

// ParseJSONLines parses JSON Lines format (newline-delimited JSON)
func (p *JSONParser) ParseJSONLines(data []byte) (*ParsedJSON, error) {
	lines := strings.Split(string(data), "\n")

	records := make([]map[string]interface{}, 0, len(lines))
	schemaFields := make(map[string]JSONField)

	for i, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		var record map[string]interface{}
		err := json.Unmarshal([]byte(line), &record)
		if err != nil {
			return nil, fmt.Errorf("failed to parse line %d: %w", i, err)
		}

		if p.config.FlattenNested {
			record = p.flattenRecord(record, "")
		}

		records = append(records, record)

		// Detect schema from first record
		if len(records) == 1 {
			for key, val := range record {
				fieldType := p.detectValueType(val)
				schemaFields[key] = JSONField{
					Name:     key,
					Type:     fieldType,
					Nullable: val == nil,
				}
			}
		}
	}

	schema := JSONSchema{
		Fields:  p.mapFieldSlice(schemaFields),
		IsArray: false,
	}

	return &ParsedJSON{
		Data:   records,
		Schema: schema,
	}, nil
}

// ParseJSONArray parses a JSON array
func (p *JSONParser) ParseJSONArray(data []byte) (*ParsedJSON, error) {
	var records []map[string]interface{}
	err := json.Unmarshal(data, &records)
	if err != nil {
		return nil, fmt.Errorf("failed to parse JSON array: %w", err)
	}

	schemaFields := make(map[string]JSONField)

	if len(records) > 0 {
		for key, val := range records[0] {
			if p.config.FlattenNested {
				val = p.flattenValue(val, "")
			}
			fieldType := p.detectValueType(val)
			schemaFields[key] = JSONField{
				Name:     key,
				Type:     fieldType,
				Nullable: val == nil,
			}
		}
	}

	// Flatten all records if needed
	if p.config.FlattenNested {
		for i, record := range records {
			records[i] = p.flattenRecord(record, "")
		}
	}

	schema := JSONSchema{
		Fields:  p.mapFieldSlice(schemaFields),
		IsArray: true,
	}

	return &ParsedJSON{
		Data:   records,
		Schema: schema,
	}, nil
}

// ParseJSONDocument parses a single JSON document
func (p *JSONParser) ParseJSONDocument(data []byte) (*ParsedJSON, error) {
	var document map[string]interface{}
	err := json.Unmarshal(data, &document)
	if err != nil {
		return nil, fmt.Errorf("failed to parse JSON document: %w", err)
	}

	if p.config.FlattenNested {
		document = p.flattenRecord(document, "")
	}

	schemaFields := make(map[string]JSONField)
	for key, val := range document {
		fieldType := p.detectValueType(val)
		schemaFields[key] = JSONField{
			Name:     key,
			Type:     fieldType,
			Nullable: val == nil,
		}
	}

	schema := JSONSchema{
		Fields:  p.mapFieldSlice(schemaFields),
		IsArray: false,
	}

	return &ParsedJSON{
		Data:   []map[string]interface{}{document},
		Schema: schema,
	}, nil
}

// DetectSchema detects schema from JSON data
func (p *JSONParser) DetectSchema(data []byte) (*JSONSchema, error) {
	// Try JSON Lines first
	if matched, _ := regexp.MatchString(`^\s*\{`, string(data)); matched {
		if parsed, err := p.ParseJSONLines(data); err == nil {
			return &parsed.Schema, nil
		}
	}

	// Try JSON Array
	if bytes.HasPrefix(data, []byte("[")) {
		if parsed, err := p.ParseJSONArray(data); err == nil {
			return &parsed.Schema, nil
		}
	}

	// Try JSON Document
	if parsed, err := p.ParseJSONDocument(data); err == nil {
		return &parsed.Schema, nil
	}

	return nil, fmt.Errorf("unable to detect JSON format")
}

// flattenRecord flattens a nested record
func (p *JSONParser) flattenRecord(record map[string]interface{}, prefix string) map[string]interface{} {
	flattened := make(map[string]interface{})

	for key, value := range record {
		newKey := key
		if prefix != "" {
			newKey = prefix + p.config.Separator + key
		}

		switch val := value.(type) {
		case map[string]interface{}:
			nested := p.flattenRecord(val, newKey)
			for nk, nv := range nested {
				flattened[nk] = nv
			}
		case []interface{}:
			// For arrays, we can either flatten or keep as-is
			// Here we keep as-is but could convert to indexed keys
			flattened[newKey] = p.flattenArray(val, newKey)
		default:
			flattened[newKey] = value
		}
	}

	return flattened
}

// flattenValue flattens a single value
func (p *JSONParser) flattenValue(value interface{}, prefix string) interface{} {
	switch val := value.(type) {
	case map[string]interface{}:
		return p.flattenRecord(val, prefix)
	case []interface{}:
		return p.flattenArray(val, prefix)
	default:
		return value
	}
}

// flattenArray flattens an array
func (p *JSONParser) flattenArray(array []interface{}, prefix string) interface{} {
	// Option 1: Keep array as-is
	// Option 2: Convert to indexed keys (e.g., field_0, field_1)
	// Here we keep as-is for simplicity
	return array
}

// detectValueType detects the type of a value
func (p *JSONParser) detectValueType(value interface{}) string {
	if value == nil {
		return "null"
	}

	switch v := value.(type) {
	case bool:
		return "boolean"
	case int, int32, int64:
		return "integer"
	case float32, float64:
		return "float"
	case string:
		// Try to detect if string represents a number
		if matched, _ := regexp.MatchString(`^-?\d+$`, v); matched {
			return "integer"
		}
		if matched, _ := regexp.MatchString(`^-?\d+\.\d+$`, v); matched {
			return "float"
		}
		// Try to detect if string represents a boolean
		if v == "true" || v == "false" {
			return "boolean"
		}
		return "string"
	case []interface{}:
		if len(v) > 0 {
			// Detect array element type
			return "array<" + p.detectValueType(v[0]) + ">"
		}
		return "array"
	case map[string]interface{}:
		return "object"
	default:
		return "string"
	}
}

// GetNestedValue retrieves a nested value using dot notation
func (p *JSONParser) GetNestedValue(record map[string]interface{}, path string) (interface{}, bool) {
	if path == "" {
		return nil, false
	}

	parts := strings.Split(path, ".")
	current := interface{}(record)

	for _, part := range parts {
		switch curr := current.(type) {
		case map[string]interface{}:
			val, exists := curr[part]
			if !exists {
				return nil, false
			}
			current = val
		case []interface{}:
			// Handle array indexing (e.g., items[0])
			// This would require parsing array indices
			return nil, false
		default:
			return nil, false
		}
	}

	return current, true
}

// QueryJSON queries JSON data with simple filtering
func (p *JSONParser) QueryJSON(data []byte, filters map[string]interface{}) ([]map[string]interface{}, error) {
	parsed, err := p.ParseJSONArray(data)
	if err != nil {
		// Try JSON Lines
		parsed, err = p.ParseJSONLines(data)
		if err != nil {
			return nil, err
		}
	}

	// Apply filters
	filtered := make([]map[string]interface{}, 0)
	for _, record := range parsed.Data {
		if p.matchesFilters(record, filters) {
			filtered = append(filtered, record)
		}
	}

	return filtered, nil
}

// matchesFilters checks if a record matches all filters
func (p *JSONParser) matchesFilters(record map[string]interface{}, filters map[string]interface{}) bool {
	for key, expectedValue := range filters {
		actualValue, exists := record[key]
		if !exists {
			return false
		}

		if !p.compareValues(actualValue, expectedValue) {
			return false
		}
	}
	return true
}

// compareValues compares two values
func (p *JSONParser) compareValues(a, b interface{}) bool {
	// Handle type conversions
	switch aVal := a.(type) {
	case int:
		switch bVal := b.(type) {
		case int:
			return aVal == bVal
		case float64:
			return float64(aVal) == bVal
		case int64:
			return int64(aVal) == bVal
		}
	case int64:
		switch bVal := b.(type) {
		case int:
			return aVal == int64(bVal)
		case float64:
			return float64(aVal) == bVal
		case int64:
			return aVal == bVal
		}
	case float64:
		switch bVal := b.(type) {
		case int:
			return aVal == float64(bVal)
		case float64:
			return aVal == bVal
		case int64:
			return aVal == float64(bVal)
		}
	case string:
		if bStr, ok := b.(string); ok {
			return aVal == bStr
		}
	case bool:
		if bBool, ok := b.(bool); ok {
			return aVal == bBool
		}
	}

	// Default to string comparison
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

// mapFieldSlice converts map to slice
func (p *JSONParser) mapFieldSlice(fields map[string]JSONField) []JSONField {
	slice := make([]JSONField, 0, len(fields))
	for _, field := range fields {
		slice = append(slice, field)
	}
	return slice
}

// ExtractPaths extracts all paths from a JSON document
func (p *JSONParser) ExtractPaths(data []byte) ([]string, error) {
	var document interface{}
	err := json.Unmarshal(data, &document)
	if err != nil {
		return nil, err
	}

	paths := make([]string, 0)
	p.extractPathsRecursive(document, "", &paths)

	return paths, nil
}

// extractPathsRecursive recursively extracts paths
func (p *JSONParser) extractPathsRecursive(value interface{}, currentPath string, paths *[]string) {
	switch val := value.(type) {
	case map[string]interface{}:
		for key, child := range val {
			newPath := key
			if currentPath != "" {
				newPath = currentPath + "." + key
			}
			*paths = append(*paths, newPath)
			p.extractPathsRecursive(child, newPath, paths)
		}
	case []interface{}:
		for i, child := range val {
			newPath := fmt.Sprintf("%s[%d]", currentPath, i)
			*paths = append(*paths, newPath)
			p.extractPathsRecursive(child, newPath, paths)
		}
	}
}

// ValidateJSON validates JSON data
func (p *JSONParser) ValidateJSON(data []byte) error {
	var js interface{}
	return json.Unmarshal(data, &js)
}

// MinifyJSON minifies JSON by removing whitespace
func (p *JSONParser) MinifyJSON(data []byte) ([]byte, error) {
	var js interface{}
	err := json.Unmarshal(data, &js)
	if err != nil {
		return nil, err
	}

	return json.Marshal(js)
}

// PrettyPrintJSON pretty-prints JSON with indentation
func (p *JSONParser) PrettyPrintJSON(data []byte, indent string) ([]byte, error) {
	var js interface{}
	err := json.Unmarshal(data, &js)
	if err != nil {
		return nil, err
	}

	return json.MarshalIndent(js, "", indent)
}

// GetJSONType returns the JSON type (array, object, or document)
func (p *JSONParser) GetJSONType(data []byte) (string, error) {
	trimmed := strings.TrimSpace(string(data))

	if strings.HasPrefix(trimmed, "[") {
		return "array", nil
	}

	if strings.HasPrefix(trimmed, "{") {
		// Check if it's JSON Lines by looking for newlines
		lines := strings.Split(trimmed, "\n")
		if len(lines) > 1 {
			return "jsonlines", nil
		}
		return "object", nil
	}

	return "", fmt.Errorf("unknown JSON format")
}

// MergeFields merges fields from multiple records
func (p *JSONParser) MergeFields(records []map[string]interface{}) []string {
	fieldSet := make(map[string]bool)

	for _, record := range records {
		for key := range record {
			fieldSet[key] = true
		}
	}

	fields := make([]string, 0, len(fieldSet))
	for field := range fieldSet {
		fields = append(fields, field)
	}

	return fields
}

// InferSchemaFromRecords infers schema from multiple records
func (p *JSONParser) InferSchemaFromRecords(records []map[string]interface{}) *JSONSchema {
	if len(records) == 0 {
		return &JSONSchema{}
	}

	fieldTypes := make(map[string]map[string]int)
	fieldNullability := make(map[string]int)

	for _, record := range records {
		for key, value := range record {
			if fieldTypes[key] == nil {
				fieldTypes[key] = make(map[string]int)
			}

			typeName := p.detectValueType(value)
			fieldTypes[key][typeName]++

			if value == nil {
				fieldNullability[key]++
			}
		}
	}

	fields := make([]JSONField, 0, len(fieldTypes))

	for key, types := range fieldTypes {
		// Get most common type
		maxCount := 0
		mostCommonType := "string"

		for typeName, count := range types {
			if count > maxCount {
				maxCount = count
				mostCommonType = typeName
			}
		}

		isNullable := fieldNullability[key] > 0

		fields = append(fields, JSONField{
			Name:     key,
			Type:     mostCommonType,
			Nullable: isNullable,
		})
	}

	return &JSONSchema{
		Fields:  fields,
		IsArray: true,
	}
}
