package sql_translator

import (
	"fmt"
	"strings"
)

// MockSQLTranslator is a mock implementation of SQLTranslator
// In a real implementation, this would interface with a SQL dialect translation library
type MockSQLTranslator struct {
	supportedDialects []string
}

// NewMockSQLTranslator creates a new mock SQL translator
func NewMockSQLTranslator() *MockSQLTranslator {
	return &MockSQLTranslator{
		supportedDialects: []string{
			string(MySQL), string(PostgreSQL), string(Trino), string(Spark),
			string(Oracle), string(SQLServer), string(Snowflake), string(SQLite),
			string(DuckDB), string(Redshift),
		},
	}
}

// Translate converts SQL from source dialect to target dialect
func (m *MockSQLTranslator) Translate(sql, sourceDialect, targetDialect string) (string, error) {
	// Validate dialects are supported
	sourceSupported := false
	targetSupported := false
	
	for _, dialect := range m.supportedDialects {
		if dialect == sourceDialect {
			sourceSupported = true
		}
		if dialect == targetDialect {
			targetSupported = true
		}
	}
	
	if !sourceSupported {
		return "", &TranslationError{
			SourceDialect: sourceDialect,
			TargetDialect: targetDialect,
			Message:       fmt.Sprintf("unsupported source dialect: %s", sourceDialect),
		}
	}
	
	if !targetSupported {
		return "", &TranslationError{
			SourceDialect: sourceDialect,
			TargetDialect: targetDialect,
			Message:       fmt.Sprintf("unsupported target dialect: %s", targetDialect),
		}
	}
	
	// In a real implementation, this would call a SQL dialect translation library
	// For now, we'll simulate some basic transformations
	translatedSQL := sql
	
	// Perform some basic dialect transformations
	translatedSQL = m.transformDialect(translatedSQL, sourceDialect, targetDialect)
	
	return translatedSQL, nil
}

// transformDialect applies basic transformations between dialects
func (m *MockSQLTranslator) transformDialect(sql, source, target string) string {
	// This is a simplified transformation logic
	// In a real implementation, we would use a proper SQL parser and transformer
	
	// Example transformations:
	// MySQL uses backticks for identifiers, others may use double quotes
	if source == string(MySQL) && target != string(MySQL) {
		// Replace MySQL backticks with double quotes or remove them based on target
		sql = strings.ReplaceAll(sql, "`", "\"")
	}
	
	if source != string(MySQL) && target == string(MySQL) {
		// Replace double quotes with backticks for MySQL
		sql = strings.ReplaceAll(sql, "\"", "`")
	}
	
	// Handle different date literal formats
	if strings.Contains(strings.ToUpper(sql), "DATE ") {
		// Basic transformation for date literals
		sql = strings.ReplaceAll(sql, "DATE '", "DATE '")
	}
	
	// Handle different string concatenation methods
	if strings.Contains(strings.ToUpper(sql), "||") {
		// Concatenation operator is similar across most dialects, but Oracle is different
		if target == string(Oracle) {
			// Oracle uses CONCAT() function or ||, but let's assume || works
		}
	}
	
	// Handle LIMIT clause variations
	if strings.Contains(strings.ToUpper(sql), "LIMIT") {
		// LIMIT is supported in MySQL, PostgreSQL, SQLite, but different in SQL Server (TOP)
		if target == string(SQLServer) && !strings.Contains(strings.ToUpper(sql), "OFFSET") {
			// Convert LIMIT to TOP for SQL Server (simplified)
			sql = m.convertLimitToTop(sql)
		}
	}
	
	// Handle different NULL handling in aggregations
	// This is where actual SQL translation logic would go
	
	return sql
}

// convertLimitToTop converts MySQL/PostgreSQL LIMIT to SQL Server TOP
func (m *MockSQLTranslator) convertLimitToTop(sql string) string {
	// This is a simplified conversion
	// A real implementation would need a proper SQL parser
	limitPos := strings.Index(strings.ToUpper(sql), " LIMIT ")
	if limitPos != -1 {
		// Find the LIMIT clause and convert it to TOP
		remaining := sql[limitPos+len(" LIMIT "):]
		var limitValue string
		
		// Extract the limit value
		for i, char := range remaining {
			if char == ' ' || char == ';' || char == '\n' || char == '\t' {
				limitValue = remaining[:i]
				break
			}
		}
		
		if limitValue == "" {
			limitValue = remaining
		}
		
		// Replace SELECT with SELECT TOP (n)
		selectPos := strings.Index(strings.ToUpper(sql), "SELECT ")
		if selectPos != -1 {
			// Insert TOP clause after SELECT
			selectClauseEnd := selectPos + len("SELECT ")
			newSQL := sql[:selectClauseEnd] + "TOP (" + limitValue + ") " + sql[selectClauseEnd:limitPos]
			
			// Remove the LIMIT clause
			newSQL += sql[limitPos:]
			
			// Remove the LIMIT part
			newSQL = strings.Replace(newSQL, " LIMIT "+limitValue, "", 1)
			
			return newSQL
		}
	}
	
	return sql
}

// SupportedDialects returns a list of supported SQL dialects
func (m *MockSQLTranslator) SupportedDialects() []string {
	return m.supportedDialects
}

// Validate checks if the SQL is valid for the given dialect
func (m *MockSQLTranslator) Validate(sql, dialect string) (bool, error) {
	// In a real implementation, this would validate SQL against the specific dialect
	// For now, we'll just check if the dialect is supported and the SQL is not empty
	
	for _, supported := range m.supportedDialects {
		if supported == dialect {
			if sql == "" {
				return false, &ValidationError{
					Dialect: dialect,
					Message: "empty SQL query",
				}
			}
			return true, nil
		}
	}
	
	return false, &ValidationError{
		Dialect: dialect,
		Message: fmt.Sprintf("unsupported dialect: %s", dialect),
	}
}