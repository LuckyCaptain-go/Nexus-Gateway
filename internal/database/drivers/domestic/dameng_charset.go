package domestic

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// DaMengCharsetHandler handles DaMeng charset conversions
type DaMengCharsetHandler struct {
	driver   *DaMengDriver
	charsets []string // Supported charsets
}

// NewDaMengCharsetHandler creates a new charset handler
func NewDaMengCharsetHandler(driver *DaMengDriver) *DaMengCharsetHandler {
	return &DaMengCharsetHandler{
		driver:   driver,
		charsets: []string{"UTF8", "GB18030", "GBK", "GB2312", "BIG5"},
	}
}

// SetCharset sets the session charset
func (h *DaMengCharsetHandler) SetCharset(ctx context.Context, db *sql.DB, charset string) error {
	charset = strings.ToUpper(charset)

	if !h.IsCharsetSupported(charset) {
		return fmt.Errorf("charset %s is not supported", charset)
	}

	_, err := db.ExecContext(ctx, fmt.Sprintf("SET CLIENT_ENCODING TO %s", charset))
	if err != nil {
		return fmt.Errorf("failed to set charset: %w", err)
	}

	return nil
}

// GetCharset retrieves the current charset
func (h *DaMengCharsetHandler) GetCharset(ctx context.Context, db *sql.DB) (string, error) {
	var charset string
	err := db.QueryRowContext(ctx, "SHOW CLIENT_ENCODING").Scan(&charset)
	if err != nil {
		return "", fmt.Errorf("failed to get charset: %w", err)
	}
	return charset, nil
}

// IsCharsetSupported checks if a charset is supported
func (h *DaMengCharsetHandler) IsCharsetSupported(charset string) bool {
	charset = strings.ToUpper(charset)
	for _, supported := range h.charsets {
		if supported == charset {
			return true
		}
	}
	return false
}

// ConvertCharset converts data from one charset to another
func (h *DaMengCharsetHandler) ConvertCharset(data []byte, fromCharset, toCharset string) ([]byte, error) {
	// For now, just return data as-is
	// In real implementation, you'd use golang.org/x/text/encoding
	return data, nil
}

// GetDatabaseCharset retrieves the database charset
func (h *DaMengCharsetHandler) GetDatabaseCharset(ctx context.Context, db *sql.DB) (string, error) {
	var charset string
	err := db.QueryRowContext(ctx, "SELECT GET_DATABASE_CHARSET()").Scan(&charset)
	if err != nil {
		return "", fmt.Errorf("failed to get database charset: %w", err)
	}
	return charset, nil
}

// SetTableCharset sets charset for a table
func (h *DaMengCharsetHandler) SetTableCharset(ctx context.Context, db *sql.DB, tableName, charset string) error {
	sql := fmt.Sprintf("ALTER TABLE %s CONVERT TO CHARACTER SET %s", tableName, charset)
	_, err := db.ExecContext(ctx, sql)
	return err
}

// GetTableCharset retrieves charset for a table
func (h *DaMengCharsetHandler) GetTableCharset(ctx context.Context, db *sql.DB, tableName string) (string, error) {
	sql := `
		SELECT CHARACTER_SET_NAME
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_NAME = ?
	`

	var charset string
	err := db.QueryRowContext(ctx, sql, tableName).Scan(&charset)
	if err != nil {
		return "", fmt.Errorf("failed to get table charset: %w", err)
	}

	return charset, nil
}

// DaMengCharsetInfo represents charset information
type DaMengCharsetInfo struct {
	DatabaseCharset string
	TableCharsets   map[string]string
	ClientCharset   string
}

// GetCharsetInfo retrieves comprehensive charset information
func (h *DaMengCharsetHandler) GetCharsetInfo(ctx context.Context, db *sql.DB) (*DaMengCharsetInfo, error) {
	dbCharset, err := h.GetDatabaseCharset(ctx, db)
	if err != nil {
		return nil, err
	}

	clientCharset, err := h.GetCharset(ctx, db)
	if err != nil {
		return nil, err
	}

	info := &DaMengCharsetInfo{
		DatabaseCharset: dbCharset,
		ClientCharset:   clientCharset,
		TableCharsets:   make(map[string]string),
	}

	return info, nil
}

// ValidateCharset validates charset configuration
func (h *DaMengCharsetHandler) ValidateCharset(config *DaMengConfig) error {
	if config.Charset == "" {
		return nil // Empty charset is OK (will use default)
	}

	if !h.IsCharsetSupported(config.Charset) {
		return fmt.Errorf("unsupported charset: %s", config.Charset)
	}

	return nil
}
