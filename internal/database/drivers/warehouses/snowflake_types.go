package warehouses

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/snowflakedb/gosnowflake"
	"nexus-gateway/internal/model"
)

// SnowflakeTypeMapper handles Snowflake-specific type mappings
type SnowflakeTypeMapper struct{}

// NewSnowflakeTypeMapper creates a new Snowflake type mapper
func NewSnowflakeTypeMapper() *SnowflakeTypeMapper {
	return &SnowflakeTypeMapper{}
}

// MapSnowflakeTypeToStandardType maps Snowflake data types to standard types
func (m *SnowflakeTypeMapper) MapSnowflakeTypeToStandardType(snowflakeType string, nullable bool) model.StandardizedType {
	// Normalize type name
	upperType := strings.ToUpper(snowflakeType)

	// Extract base type (remove precision/scale)
	baseType := upperType
	if idx := strings.Index(upperType, "("); idx != -1 {
		baseType = upperType[:idx]
	}

	switch baseType {
	case "NUMBER":
		// NUMBER without precision/scale -> INTEGER
		if !strings.Contains(upperType, "(") {
			return model.StandardizedTypeInteger
		}
		// NUMBER(p,s) or NUMBER(p) -> DECIMAL based on scale
		return model.StandardizedTypeDecimal
	case "DECIMAL", "NUMERIC":
		return model.StandardizedTypeDecimal
	case "FLOAT", "FLOAT4", "FLOAT8":
		return model.StandardizedTypeFloat
	case "DOUBLE", "DOUBLE PRECISION":
		return model.StandardizedTypeDouble
	case "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT":
		return model.StandardizedTypeInteger
	case "VARCHAR", "CHAR", "CHARACTER", "STRING", "TEXT":
		return model.StandardizedTypeString
	case "BOOLEAN":
		return model.StandardizedTypeBoolean
	case "DATE":
		return model.StandardizedTypeDate
	case "TIMESTAMP", "TIMESTAMP_NTZ":
		return model.StandardizedTypeTimestamp
	case "TIMESTAMP_LTZ", "TIMESTAMP_TZ":
		return model.StandardizedTypeTimestampWithZone
	case "TIME":
		return model.StandardizedTypeTime
	case "VARIANT", "OBJECT":
		return model.StandardizedTypeVariant
	case "ARRAY":
		return model.StandardizedTypeArray
	case "BINARY", "VARBINARY":
		return model.StandardizedTypeBinary
	case "GEOGRAPHY", "GEOMETRY":
		return model.StandardizedTypeGeography
	default:
		return model.StandardizedTypeString // Default fallback
	}
}

// MapStandardTypeToSnowflakeType maps standard types to Snowflake types
func (m *SnowflakeTypeMapper) MapStandardTypeToSnowflakeType(standardType model.StandardizedType) string {
	switch standardType {
	case model.StandardizedTypeBoolean:
		return "BOOLEAN"
	case model.StandardizedTypeInteger:
		return "NUMBER(38,0)"
	case model.StandardizedTypeLong:
		return "NUMBER(38,0)"
	case model.StandardizedTypeFloat:
		return "FLOAT"
	case model.StandardizedTypeDouble:
		return "DOUBLE"
	case model.StandardizedTypeDecimal:
		return "NUMBER(38,18)"
	case model.StandardizedTypeString, model.StandardizedTypeText:
		return "VARCHAR(16777216)" // Max size
	case model.StandardizedTypeDate:
		return "DATE"
	case model.StandardizedTypeTime:
		return "TIME(9)"
	case model.StandardizedTypeTimestamp, model.StandardizedTypeDateTime:
		return "TIMESTAMP_NTZ(9)"
	case model.StandardizedTypeTimestampWithZone:
		return "TIMESTAMP_TZ(9)"
	case model.StandardizedTypeBinary:
		return "BINARY(8388608)"
	case model.StandardizedTypeVariant:
		return "VARIANT"
	case model.StandardizedTypeArray:
		return "ARRAY"
	case model.StandardizedTypeStruct, model.StandardizedTypeObject:
		return "OBJECT"
	default:
		return "VARCHAR(16777216)"
	}
}

// ConvertSnowflakeValue converts Snowflake driver value to standard format
func (m *SnowflakeTypeMapper) ConvertSnowflakeValue(value interface{}, snowflakeType string) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	// Handle VARIANT, ARRAY, OBJECT types (stored as strings by driver)
	baseType := getBaseType(snowflakeType)

	switch baseType {
	case "VARIANT":
		// VARIANT is returned as JSON string
		return value, nil
	case "ARRAY":
		// ARRAY is returned as JSON string
		return value, nil
	case "OBJECT":
		// OBJECT is returned as JSON string
		return value, nil
	case "BOOLEAN":
		// Convert to bool
		if b, ok := value.(bool); ok {
			return b, nil
		}
		if s, ok := value.(string); ok {
			return strings.ToUpper(s) == "TRUE", nil
		}
		return value, nil
	case "NUMBER", "DECIMAL":
		// Handle numeric types
		switch v := value.(type) {
		case int, int64, float64:
			return v, nil
		case string:
			// Try to parse as int first
			if i, err := strconv.ParseInt(v, 10, 64); err == nil {
				return i, nil
			}
			// Then try float
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				return f, nil
			}
			return v, nil
		default:
			return v, nil
		}
	default:
		return value, nil
	}
}

// ScanSnowflakeValue properly scans values from Snowflake
func ScanSnowflakeValue(rows *sql.Rows, columnType string) (interface{}, error) {
	// Use gosnowflake's specific scanning if needed
	// For most types, standard scanning works
	var value interface{}
	err := rows.Scan(&value)
	return value, err
}

// GetSnowflakeTypeFromValue infers Snowflake type from Go value
func (m *SnowflakeTypeMapper) GetSnowflakeTypeFromValue(value interface{}) string {
	if value == nil {
		return "VARIANT"
	}

	switch v := value.(type) {
	case bool:
		return "BOOLEAN"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return "NUMBER(38,0)"
	case float32, float64:
		return "FLOAT"
	case string:
		return "VARCHAR(16777216)"
	case []byte:
		return "BINARY"
	case time.Time:
		return "TIMESTAMP_NTZ(9)"
	default:
		// Check for slices (arrays)
		if reflect.TypeOf(value).Kind() == reflect.Slice {
			return "ARRAY"
		}
		// Check for maps (objects)
		if reflect.TypeOf(value).Kind() == reflect.Map {
			return "OBJECT"
		}
		return "VARIANT"
	}
}

// ParseTypeDefinition parses a Snowflake type definition
func (m *SnowflakeTypeMapper) ParseTypeDefinition(typeDef string) (baseType string, precision, scale int, err error) {
	typeDef = strings.TrimSpace(typeDef)

	// Find opening parenthesis
	idx := strings.Index(typeDef, "(")
	if idx == -1 {
		return typeDef, 0, 0, nil
	}

	baseType = strings.TrimSpace(typeDef[:idx])
	params := strings.TrimSuffix(typeDef[idx+1:], ")")

	// Parse precision and scale
	if strings.Contains(params, ",") {
		parts := strings.Split(params, ",")
		if len(parts) == 2 {
			p, err1 := strconv.Atoi(strings.TrimSpace(parts[0]))
			s, err2 := strconv.Atoi(strings.TrimSpace(parts[1]))
			if err1 != nil || err2 != nil {
				return baseType, 0, 0, fmt.Errorf("invalid precision/scale: %s", params)
			}
			return baseType, p, s, nil
		}
	} else {
		p, err := strconv.Atoi(strings.TrimSpace(params))
		if err != nil {
			return baseType, 0, 0, nil // Ignore parsing error for non-numeric params
		}
		return baseType, p, 0, nil
	}

	return baseType, 0, 0, nil
}

// GetColumnSize returns the column size for display
func (m *SnowflakeTypeMapper) GetColumnSize(snowflakeType string) int {
	baseType, precision, _, err := NewSnowflakeTypeMapper().ParseTypeDefinition(snowflakeType)
	if err != nil {
		return 0
	}

	switch baseType {
	case "VARCHAR", "CHAR", "BINARY", "VARBINARY":
		if precision > 0 {
			return precision
		}
		return 16777216 // Max size
	case "NUMBER", "DECIMAL":
		if precision > 0 {
			return precision
		}
		return 38
	default:
		return 0
	}
}

// SupportsTypeChecking checks if type supports certain operations
func (m *SnowflakeTypeMapper) SupportsTypeChecking(snowflakeType, operation string) bool {
	baseType := getBaseType(snowflakeType)

	switch operation {
	case "comparison":
		return baseType != "VARIANT" && baseType != "OBJECT" && baseType != "ARRAY"
	case "ordering":
		return baseType != "VARIANT" && baseType != "OBJECT" && baseType != "ARRAY" && baseType != "GEOGRAPHY"
	case "aggregation":
		return baseType != "VARIANT" && baseType != "OBJECT" && baseType != "ARRAY"
	case "grouping":
		return baseType != "VARIANT" && baseType != "OBJECT" && baseType != "ARRAY"
	default:
		return true
	}
}

// IsSemiStructuredType checks if type is semi-structured
func (m *SnowflakeTypeMapper) IsSemiStructuredType(snowflakeType string) bool {
	baseType := getBaseType(snowflakeType)
	return baseType == "VARIANT" || baseType == "OBJECT" || baseType == "ARRAY"
}

// GetTypeAffinity returns type affinity for operations
func (m *SnowflakeTypeMapper) GetTypeAffinity(snowflakeType string) TypeAffinity {
	baseType := getBaseType(snowflakeType)

	switch baseType {
	case "NUMBER", "DECIMAL", "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "FLOAT", "DOUBLE":
		return AffinityNumeric
	case "VARCHAR", "CHAR", "STRING", "TEXT":
		return AffinityString
	case "BOOLEAN":
		return AffinityBoolean
	case "DATE", "TIME", "TIMESTAMP":
		return AffinityDateTime
	case "VARIANT", "OBJECT", "ARRAY":
		return AffinitySemiStructured
	default:
		return AffinityUnknown
	}
}

// TypeAffinity represents type affinity categories
type TypeAffinity int

const (
	AffinityUnknown TypeAffinity = iota
	AffinityNumeric
	AffinityString
	AffinityBoolean
	AffinityDateTime
	AffinitySemiStructured
)

// Helper function to get base type from full type definition
func getBaseType(snowflakeType string) string {
	upperType := strings.ToUpper(snowflakeType)
	if idx := strings.Index(upperType, "("); idx != -1 {
		return upperType[:idx]
	}
	return upperType
}

// GetTypeInfo returns detailed type information
func (m *SnowflakeTypeMapper) GetTypeInfo(snowflakeType string) *SnowflakeTypeInfo {
	baseType, precision, scale, _ := m.ParseTypeDefinition(snowflakeType)

	info := &SnowflakeTypeInfo{
		BaseType:       baseType,
		FullType:       snowflakeType,
		Precision:      precision,
		Scale:          scale,
		IsSemiStructured: m.IsSemiStructuredType(snowflakeType),
		Affinity:       m.GetTypeAffinity(snowflakeType),
	}

	return info
}

// SnowflakeTypeInfo contains detailed type information
type SnowflakeTypeInfo struct {
	BaseType         string
	FullType         string
	Precision        int
	Scale            int
	IsSemiStructured bool
	Affinity         TypeAffinity
}


