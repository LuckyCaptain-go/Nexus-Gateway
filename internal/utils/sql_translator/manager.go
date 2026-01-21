package sql_translator

import (
	"fmt"
	"strings"
)

// SQLTranslationManager manages SQL dialect translation
type SQLTranslationManager struct {
	translator SQLTranslator
}

// NewSQLTranslationManager creates a new SQL translation manager
func NewSQLTranslationManager(useExternal bool) *SQLTranslationManager {
	return &SQLTranslationManager{
		translator: GetTranslator(useExternal),
	}
}

// TranslateQuery translates a SQL query from a source dialect to the target database dialect
func (stm *SQLTranslationManager) TranslateQuery(sql string, sourceDialect, targetDialect string) (string, error) {
	// Validate inputs
	if sql == "" {
		return "", fmt.Errorf("empty SQL query")
	}
	if sourceDialect == "" {
		return "", fmt.Errorf("empty source dialect")
	}
	if targetDialect == "" {
		return "", fmt.Errorf("empty target dialect")
	}

	// Normalize dialect names
	sourceDialect = strings.ToLower(sourceDialect)
	targetDialect = strings.ToLower(targetDialect)

	// If source and target are the same, return original SQL
	if sourceDialect == targetDialect {
		return sql, nil
	}

	// Translate the SQL
	translatedSQL, err := stm.translator.Translate(sql, sourceDialect, targetDialect)
	if err != nil {
		return "", fmt.Errorf("failed to translate SQL from %s to %s: %w", sourceDialect, targetDialect, err)
	}

	return translatedSQL, nil
}

// GetSupportedDialects returns all supported SQL dialects
func (stm *SQLTranslationManager) GetSupportedDialects() []string {
	return stm.translator.SupportedDialects()
}

// ValidateSQL validates SQL against a specific dialect
func (stm *SQLTranslationManager) ValidateSQL(sql, dialect string) (bool, error) {
	return stm.translator.Validate(sql, dialect)
}

// TranslateFromMySQL translates MySQL SQL to another dialect
func (stm *SQLTranslationManager) TranslateFromMySQL(sql, targetDialect string) (string, error) {
	return stm.TranslateQuery(sql, string(MySQL), targetDialect)
}

// TranslateFromTrino translates Trino SQL to another dialect
func (stm *SQLTranslationManager) TranslateFromTrino(sql, targetDialect string) (string, error) {
	return stm.TranslateQuery(sql, string(Trino), targetDialect)
}

// TranslateToMySQL translates SQL from any dialect to MySQL
func (stm *SQLTranslationManager) TranslateToMySQL(sql, sourceDialect string) (string, error) {
	return stm.TranslateQuery(sql, sourceDialect, string(MySQL))
}

// TranslateToTrino translates SQL from any dialect to Trino
func (stm *SQLTranslationManager) TranslateToTrino(sql, sourceDialect string) (string, error) {
	return stm.TranslateQuery(sql, sourceDialect, string(Trino))
}