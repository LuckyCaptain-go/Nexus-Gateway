package domestic

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// GBaseCharsetHandler handles GBase charset conversions
type GBaseCharsetHandler struct {
	driver *GBaseDriver
}

// NewGBaseCharsetHandler creates a new charset handler
func NewGBaseCharsetHandler(driver *GBaseDriver) *GBaseCharsetHandler {
	return &GBaseCharsetHandler{
		driver: driver,
	}
}

// SetCharset sets the session charset
func (h *GBaseCharsetHandler) SetCharset(ctx context.Context, db *sql.DB, charset string) error {
	charset = strings.ToUpper(charset)

	_, err := db.ExecContext(ctx, fmt.Sprintf("SET NAMES %s", charset))
	if err != nil {
		return fmt.Errorf("failed to set charset: %w", err)
	}

	return nil
}

// GetCharset retrieves the current charset
func (h *GBaseCharsetHandler) GetCharset(ctx context.Context, db *sql.DB) (string, error) {
	var charset string
	err := db.QueryRowContext(ctx, "SHOW VARIABLES LIKE 'character_set_connection'").Scan(nil, &charset)
	if err != nil {
		return "", fmt.Errorf("failed to get charset: %w", err)
	}
	return charset, nil
}

// GetDatabaseCharset retrieves the database charset
func (h *GBaseCharsetHandler) GetDatabaseCharset(ctx context.Context, db *sql.DB, databaseName string) (string, error) {
	sql := `
		SELECT DEFAULT_CHARACTER_SET_NAME
		FROM information_schema.SCHEMATA
		WHERE SCHEMA_NAME = ?
	`

	var charset string
	err := db.QueryRowContext(ctx, sql, databaseName).Scan(&charset)
	if err != nil {
		return "", fmt.Errorf("failed to get database charset: %w", err)
	}

	return charset, nil
}

// GetTableCharset retrieves charset for a table
func (h *GBaseCharsetHandler) GetTableCharset(ctx context.Context, db *sql.DB, tableName string) (string, error) {
	sql := `
		SELECT CHARACTER_SET_NAME
		FROM information_schema.TABLES
		WHERE TABLE_NAME = ?
	`

	var charset string
	err := db.QueryRowContext(ctx, sql, tableName).Scan(&charset)
	if err != nil {
		return "", fmt.Errorf("failed to get table charset: %w", err)
	}

	return charset, nil
}

// ConvertCharset converts data between charsets
func (h *GBaseCharsetHandler) ConvertCharset(ctx context.Context, db *sql.DB, data []byte, fromCharset, toCharset string) ([]byte, error) {
	// Use CONVERT function
	sql := fmt.Sprintf("SELECT CONVERT(%s USING %s)", string(data), toCharset)
	var result string
	err := db.QueryRowContext(ctx, sql).Scan(&result)
	if err != nil {
		return nil, fmt.Errorf("failed to convert charset: %w", err)
	}
	return []byte(result), nil
}

// SetCollation sets the collation
func (h *GBaseCharsetHandler) SetCollation(ctx context.Context, db *sql.DB, collation string) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf("SET collation_connection = '%s'", collation))
	if err != nil {
		return fmt.Errorf("failed to set collation: %w", err)
	}
	return nil
}

// GetCollation retrieves the current collation
func (h *GBaseCharsetHandler) GetCollation(ctx context.Context, db *sql.DB) (string, error) {
	var collation string
	err := db.QueryRowContext(ctx, "SHOW VARIABLES LIKE 'collation_connection'").Scan(nil, &collation)
	if err != nil {
		return "", fmt.Errorf("failed to get collation: %w", err)
	}
	return collation, nil
}

// GetSupportedCharsets retrieves list of supported charsets
func (h *GBaseCharsetHandler) GetSupportedCharsets(ctx context.Context, db *sql.DB) ([]string, error) {
	sql := "SELECT CHARACTER_SET_NAME FROM information_schema.CHARACTER_SETS"

	rows, err := db.QueryContext(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to get supported charsets: %w", err)
	}
	defer rows.Close()

	var charsets []string

	for rows.Next() {
		var charset string
		err := rows.Scan(&charset)
		if err != nil {
			return nil, err
		}
		charsets = append(charsets, charset)
	}

	return charsets, nil
}

// GBaseCharsetInfo represents charset information
type GBaseCharsetInfo struct {
	Charset    string
	Description string
	DefaultCollation string
	MaxLength   int
}

// GetCharsetInfo retrieves information about a specific charset
func (h *GBaseCharsetHandler) GetCharsetInfo(ctx context.Context, db *sql.DB, charset string) (*GBaseCharsetInfo, error) {
	sql := `
		SELECT CHARACTER_SET_NAME, DESCRIPTION, DEFAULT_COLLATE_NAME, MAXLEN
		FROM information_schema.CHARACTER_SETS
		WHERE CHARACTER_SET_NAME = ?
	`

	info := &GBaseCharsetInfo{
		Charset: charset,
	}

	err := db.QueryRowContext(ctx, sql, charset).Scan(
		&info.Charset,
		&info.Description,
		&info.DefaultCollation,
		&info.MaxLength,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get charset info: %w", err)
	}

	return info, nil
}
