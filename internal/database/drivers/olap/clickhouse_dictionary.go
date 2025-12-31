package olap

import (
	"context"
	"database/sql"
	"fmt"
)

// ClickHouseDictionaryManager handles ClickHouse dictionaries
type ClickHouseDictionaryManager struct {
	driver *ClickHouseDriver
}

// NewClickHouseDictionaryManager creates a new dictionary manager
func NewClickHouseDictionaryManager(driver *ClickHouseDriver) *ClickHouseDictionaryManager {
	return &ClickHouseDictionaryManager{
		driver: driver,
	}
}

// CreateDictionary creates a dictionary
func (m *ClickHouseDictionaryManager) CreateDictionary(ctx context.Context, db *sql.DB, dictionary *ClickHouseDictionary) error {
	sql := fmt.Sprintf("CREATE DICTIONARY %s (%s)",
		dictionary.Name,
		formatDictionarySchema(dictionary))

	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to create dictionary: %w", err)
	}

	return nil
}

// ClickHouseDictionary represents a dictionary definition
type ClickHouseDictionary struct {
	Name       string
	Layout     string // FLAT, HASHED, SPARSE_HASHED, COMPLEX_KEY_HASHED, RANGE_HASHED
	Source     string // HTTP, MYSQL, POSTGRESQL, CLICKHOUSE, FILE, SQLITE
	Fields     []ClickHouseDictionaryField
	PrimaryKey []string
	Lifetime   ClickHouseDictionaryLifetime
}

// ClickHouseDictionaryField represents a dictionary field
type ClickHouseDictionaryField struct {
	Name      string
	Type      string
	Nullable  bool
	Expressed bool // Is calculated expression
}

// ClickHouseDictionaryLifetime represents dictionary lifetime
type ClickHouseDictionaryLifetime struct {
	Min int64
	Max int64
}

// formatDictionarySchema formats dictionary schema
func formatDictionarySchema(dict *ClickHouseDictionary) string {
	schema := ""
	for i, field := range dict.Fields {
		if i > 0 {
			schema += ", "
		}
		nullable := ""
		if field.Nullable {
			nullable = "Nullable("
		}
		schema += fmt.Sprintf("%s %s%s)", field.Name, nullable, field.Type)
	}

	// Add primary key if present
	if len(dict.PrimaryKey) > 0 {
		schema += ", PRIMARY KEY ("
		for i, key := range dict.PrimaryKey {
			if i > 0 {
				schema += ", "
			}
			schema += key
		}
		schema += ")"
	}

	return schema
}

// DropDictionary drops a dictionary
func (m *ClickHouseDictionaryManager) DropDictionary(ctx context.Context, db *sql.DB, dictionaryName string) error {
	sql := fmt.Sprintf("DROP DICTIONARY %s", dictionaryName)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to drop dictionary: %w", err)
	}
	return nil
}

// ReloadDictionary reloads a dictionary
func (m *ClickHouseDictionaryManager) ReloadDictionary(ctx context.Context, db *sql.DB, dictionaryName string) error {
	sql := fmt.Sprintf("SYSTEM RELOAD DICTIONARY %s", dictionaryName)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to reload dictionary: %w", err)
	}
	return nil
}

// ListDictionaries lists all dictionaries
func (m *ClickHouseDictionaryManager) ListDictionaries(ctx context.Context, db *sql.DB) ([]ClickHouseDictionaryInfo, error) {
	sql := `
		SELECT name, type, origin, key, layout
		FROM system.dictionaries
	`

	rows, err := db.QueryContext(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to list dictionaries: %w", err)
	}
	defer rows.Close()

	var dictionaries []ClickHouseDictionaryInfo

	for rows.Next() {
		var dict ClickHouseDictionaryInfo
		err := rows.Scan(
			&dict.Name,
			&dict.Type,
			&dict.Origin,
			&dict.Key,
			&dict.Layout,
		)
		if err != nil {
			return nil, err
		}
		dictionaries = append(dictionaries, dict)
	}

	return dictionaries, nil
}

// ClickHouseDictionaryInfo represents dictionary information
type ClickHouseDictionaryInfo struct {
	Name   string
	Type   string
	Origin string
	Key    string
	Layout string
	Bytes  uint64
	Rows   uint64
}

// GetDictionaryInfo retrieves information about a dictionary
func (m *ClickHouseDictionaryManager) GetDictionaryInfo(ctx context.Context, db *sql.DB, dictionaryName string) (*ClickHouseDictionaryInfo, error) {
	sql := `
		SELECT name, type, origin, key, layout, bytes, rows
		FROM system.dictionaries
		WHERE name = ?
	`

	info := &ClickHouseDictionaryInfo{
		Name: dictionaryName,
	}

	err := db.QueryRowContext(ctx, sql, dictionaryName).Scan(
		&info.Name,
		&info.Type,
		&info.Origin,
		&info.Key,
		&info.Layout,
		&info.Bytes,
		&info.Rows,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get dictionary info: %w", err)
	}

	return info, nil
}
