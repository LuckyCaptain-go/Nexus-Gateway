package common

// TableSchema represents the schema of a single table
type TableSchema struct {
	Name        string                 `json:"name"`
	Schema      string                 `json:"schema,omitempty"`
	Type        string                 `json:"type,omitempty"`
	Columns     []ColumnSchema         `json:"columns"`
	PrimaryKey  []string               `json:"primaryKey,omitempty"`
	Indexes     []IndexSchema          `json:"indexes,omitempty"`
	ForeignKeys []ForeignKeySchema     `json:"foreignKeys,omitempty"`
	Properties  map[string]interface{} `json:"properties,omitempty"`
}

// ColumnSchema represents a column definition
type ColumnSchema struct {
	Name         string      `json:"name"`
	Type         string      `json:"type"`
	Nullable     bool        `json:"nullable"`
	DefaultValue interface{} `json:"defaultValue,omitempty"`
	Comment      string      `json:"comment,omitempty"`
}

// IndexSchema represents an index definition
type IndexSchema struct {
	Name    string   `json:"name"`
	Columns []string `json:"columns"`
	Unique  bool     `json:"unique"`
}

// ForeignKeySchema represents a foreign key definition
type ForeignKeySchema struct {
	Name         string   `json:"name"`
	Columns      []string `json:"columns"`
	ReferTable   string   `json:"referTable"`
	ReferColumns []string `json:"referColumns"`
}

// DataSourceSchema represents a complete data source schema
type DataSourceSchema struct {
	Tables map[string]*TableSchema
}
