package olap

import (
	"fmt"
	"reflect"
	"strings"
)

// ClickHouseArrayMapper handles ClickHouse Array type conversions
type ClickHouseArrayMapper struct {
	useSlice bool
}

// NewClickHouseArrayMapper creates a new Array type mapper
func NewClickHouseArrayMapper(useSlice bool) *ClickHouseArrayMapper {
	return &ClickHouseArrayMapper{
		useSlice: useSlice,
	}
}

// MapArrayToInterface maps ClickHouse Array to Go interface{}
func (m *ClickHouseArrayMapper) MapArrayToInterface(arrayType string, value interface{}) (interface{}, error) {
	if value == nil {
		return nil, nil
	}

	// Get element type
	elemType := m.extractElementType(arrayType)

	switch elemType {
	case "UInt8", "UInt16", "UInt32", "UInt64",
		"Int8", "Int16", "Int32", "Int64",
		"Float32", "Float64":
		return m.mapNumericArray(value)
	case "String", "FixedString":
		return m.mapStringArray(value)
	case "Date", "DateTime", "DateTime64":
		return m.mapDateArray(value)
	case "Array":
		return m.mapNestedArray(value)
	default:
		return value, nil
	}
}

// extractElementType extracts element type from Array(T)
func (m *ClickHouseArrayMapper) extractElementType(arrayType string) string {
	arrayType = strings.TrimSpace(arrayType)
	if strings.HasPrefix(arrayType, "Array(") && strings.HasSuffix(arrayType, ")") {
		return arrayType[6 : len(arrayType)-1]
	}
	return "String"
}

// mapNumericArray maps numeric arrays
func (m *ClickHouseArrayMapper) mapNumericArray(value interface{}) (interface{}, error) {
	rv := reflect.ValueOf(value)

	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		length := rv.Len()
		result := make([]interface{}, length)
		for i := 0; i < length; i++ {
			result[i] = rv.Index(i).Interface()
		}
		return result, nil
	default:
		return []interface{}{value}, nil
	}
}

// mapStringArray maps string arrays
func (m *ClickHouseArrayMapper) mapStringArray(value interface{}) (interface{}, error) {
	rv := reflect.ValueOf(value)

	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		length := rv.Len()
		result := make([]string, length)
		for i := 0; i < length; i++ {
			elem := rv.Index(i).Interface()
			result[i] = fmt.Sprintf("%v", elem)
		}
		return result, nil
	default:
		return []string{fmt.Sprintf("%v", value)}, nil
	}
}

// mapDateArray maps date/datetime arrays
func (m *ClickHouseArrayMapper) mapDateArray(value interface{}) (interface{}, error) {
	return m.mapNumericArray(value)
}

// mapNestedArray maps nested arrays (Array(Array(T)))
func (m *ClickHouseArrayMapper) mapNestedArray(value interface{}) (interface{}, error) {
	rv := reflect.ValueOf(value)

	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		length := rv.Len()
		result := make([]interface{}, length)
		for i := 0; i < length; i++ {
			nested, err := m.mapNumericArray(rv.Index(i).Interface())
			if err != nil {
				return nil, err
			}
			result[i] = nested
		}
		return result, nil
	default:
		return [][]interface{}{{value}}, nil
	}
}

// ArrayTypeInfo holds array type information
type ArrayTypeInfo struct {
	BaseType    string
	Dimensions  int
	IsNullable  bool
	Size        int
}

// ParseArrayType parses Array(T) type string
func (m *ClickHouseArrayMapper) ParseArrayType(typeStr string) (*ArrayTypeInfo, error) {
	typeStr = strings.TrimSpace(typeStr)
	isNullable := strings.HasPrefix(typeStr, "Nullable(")
	if isNullable {
		typeStr = strings.TrimPrefix(typeStr, "Nullable(")
		typeStr = strings.TrimSuffix(typeStr, ")")
	}

	dimensions := 0
	for strings.HasPrefix(typeStr, "Array(") {
		dimensions++
		typeStr = strings.TrimPrefix(typeStr, "Array(")
		typeStr = strings.TrimSuffix(typeStr, ")")
	}

	return &ArrayTypeInfo{
		BaseType:   typeStr,
		Dimensions: dimensions,
		IsNullable: isNullable,
	}, nil
}

// BuildArrayLiteral builds ClickHouse array literal
func (m *ClickHouseArrayMapper) BuildArrayLiteral(elements []interface{}) string {
	if len(elements) == 0 {
		return "[]"
	}

	strElems := make([]string, len(elements))
	for i, elem := range elements {
		switch v := elem.(type) {
		case string:
			strElems[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(v, "'", "\\'"))
		case nil:
			strElems[i] = "NULL"
		default:
			strElems[i] = fmt.Sprintf("%v", v)
		}
	}

	return fmt.Sprintf("[%s]", strings.Join(strElems, ", "))
}

// FlattenArray flattens nested arrays to 1D
func (m *ClickHouseArrayMapper) FlattenArray(arr interface{}) []interface{} {
	rv := reflect.ValueOf(arr)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return []interface{}{arr}
	}

	result := make([]interface{}, 0)
	for i := 0; i < rv.Len(); i++ {
		elem := rv.Index(i).Interface()
		elemRv := reflect.ValueOf(elem)

		if elemRv.Kind() == reflect.Slice || elemRv.Kind() == reflect.Array {
			result = append(result, m.FlattenArray(elem)...)
		} else {
			result = append(result, elem)
		}
	}

	return result
}

// GetArrayDimensions calculates the dimensions of nested arrays
func (m *ClickHouseArrayMapper) GetArrayDimensions(arr interface{}) int {
	rv := reflect.ValueOf(arr)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return 0
	}

	if rv.Len() == 0 {
		return 1
	}

	firstElem := rv.Index(0).Interface()
	firstElemRv := reflect.ValueOf(firstElem)

	if firstElemRv.Kind() == reflect.Slice || firstElemRv.Kind() == reflect.Array {
		return 1 + m.GetArrayDimensions(firstElem)
	}

	return 1
}
