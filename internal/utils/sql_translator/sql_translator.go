package sql_translator

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
)

// ExternalSQLTranslator interfaces with an external SQL dialect translation service
// This could be a Python script using sqlglot or another translation library
type ExternalSQLTranslator struct {
	supportedDialects []string
	useExternalLib    bool
}

// NewExternalSQLTranslator creates a new external SQL translator
func NewExternalSQLTranslator(useExternalLib bool) *ExternalSQLTranslator {
	return &ExternalSQLTranslator{
		supportedDialects: []string{
			string(MySQL), string(PostgreSQL), string(Trino), string(Spark),
			string(Oracle), string(SQLServer), string(Snowflake), string(SQLite),
			string(DuckDB), string(Redshift),
		},
		useExternalLib: useExternalLib,
	}
}

// Translate converts SQL from source dialect to target dialect using external library if available
func (e *ExternalSQLTranslator) Translate(sql, sourceDialect, targetDialect string) (string, error) {
	if e.useExternalLib {
		return e.translateWithExternalLib(sql, sourceDialect, targetDialect)
	}
	
	// Fallback to mock implementation
	mockTranslator := NewMockSQLTranslator()
	return mockTranslator.Translate(sql, sourceDialect, targetDialect)
}

// translateWithExternalLib calls an external SQL translation library (like Python sqlglot)
func (e *ExternalSQLTranslator) translateWithExternalLib(sql, sourceDialect, targetDialect string) (string, error) {
	// Prepare the translation request
	request := map[string]string{
		"sql":           sql,
		"source_dialect": strings.ToLower(sourceDialect),
		"target_dialect": strings.ToLower(targetDialect),
	}
	
	requestBytes, err := json.Marshal(request)
	if err != nil {
		return "", &TranslationError{
			SourceDialect: sourceDialect,
			TargetDialect: targetDialect,
			Message:       fmt.Sprintf("failed to marshal translation request: %v", err),
		}
	}
	
	// Call the Python script that uses sqlglot
	cmd := exec.Command("python3", "./scripts/sql_translate.py")
	cmd.Stdin = bytes.NewReader(requestBytes)
	
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	
	err = cmd.Run()
	if err != nil {
		return "", &TranslationError{
			SourceDialect: sourceDialect,
			TargetDialect: targetDialect,
			Message:       fmt.Sprintf("external translation failed: %v, stderr: %s", err, stderr.String()),
		}
	}
	
	// Parse the response
	var response map[string]interface{}
	if err := json.Unmarshal(stdout.Bytes(), &response); err != nil {
		return "", &TranslationError{
			SourceDialect: sourceDialect,
			TargetDialect: targetDialect,
			Message:       fmt.Sprintf("failed to parse translation response: %v", err),
		}
	}
	
	// Check for errors in the response
	if errorMsg, hasError := response["error"]; hasError {
		return "", &TranslationError{
			SourceDialect: sourceDialect,
			TargetDialect: targetDialect,
			Message:       fmt.Sprintf("translation error: %v", errorMsg),
		}
	}
	
	// Get the translated SQL
	if translatedSQL, hasResult := response["translated_sql"]; hasResult {
		if result, ok := translatedSQL.(string); ok {
			return result, nil
		}
	}
	
	return "", &TranslationError{
		SourceDialect: sourceDialect,
		TargetDialect: targetDialect,
		Message:       "malformed response from translation service",
	}
}

// simulateTranslation simulates what a real SQL translation might look like
func (e *ExternalSQLTranslator) simulateTranslation(sql, sourceDialect, targetDialect string) string {
	// This simulates what an external SQL dialect translator might do
	// In a real implementation, this would be handled by the external library
	
	// Basic transformations
	translatedSQL := sql
	
	// Handle identifier quoting differences
	switch {
	case sourceDialect == string(MySQL) && targetDialect != string(MySQL):
		// MySQL uses backticks, convert to standard quotes
		translatedSQL = strings.ReplaceAll(translatedSQL, "`", "\"")
	case sourceDialect != string(MySQL) && targetDialect == string(MySQL):
		// Convert to MySQL backticks
		translatedSQL = strings.ReplaceAll(translatedSQL, "\"", "`")
	}
	
	// Handle LIMIT/TOP differences
	if targetDialect == string(SQLServer) && strings.Contains(strings.ToUpper(translatedSQL), "LIMIT") {
		// Convert LIMIT to TOP for SQL Server
		translatedSQL = e.convertLimitToTop(translatedSQL)
	}
	
	// Handle date/time function differences
	translatedSQL = e.convertDateTimeFunctions(translatedSQL, sourceDialect, targetDialect)
	
	// Handle string function differences
	translatedSQL = e.convertStringFunctions(translatedSQL, sourceDialect, targetDialect)
	
	// Add dialect-specific comment
	translatedSQL = fmt.Sprintf("-- Translated from %s to %s\n%s", sourceDialect, targetDialect, translatedSQL)
	
	return translatedSQL
}

// convertLimitToTop converts LIMIT clause to SQL Server TOP clause
func (e *ExternalSQLTranslator) convertLimitToTop(sql string) string {
	upperSQL := strings.ToUpper(sql)
	limitIdx := strings.LastIndex(upperSQL, " LIMIT ")
	
	if limitIdx == -1 {
		return sql // No LIMIT clause found
	}
	
	// Extract the limit value
	limitPart := sql[limitIdx+len(" LIMIT "):]
	var limitValue string
	
	// Find where the limit value ends (space, semicolon, EOF)
	for i, char := range limitPart {
		if char == ' ' || char == ';' || char == '\n' || char == '\r' {
			limitValue = strings.TrimSpace(limitPart[:i])
			break
		}
	}
	
	// If no space was found, take the rest as the limit value
	if limitValue == "" {
		limitValue = strings.TrimSpace(limitPart)
	}
	
	// Find the SELECT statement to insert TOP
	selectIdx := strings.Index(upperSQL, "SELECT ")
	if selectIdx == -1 {
		return sql // No SELECT found
	}
	
	// Find where SELECT clause ends (after first space after SELECT)
	selectEnd := selectIdx + len("SELECT ")
	
	// Look for DISTINCT or the first field
	afterSelect := sql[selectEnd:]
	if strings.HasPrefix(strings.ToUpper(afterSelect), "DISTINCT ") {
		selectEnd += len("DISTINCT ")
	}
	
	// Insert TOP after SELECT
	newSQL := sql[:selectEnd] + "TOP (" + limitValue + ") " + sql[selectEnd:limitIdx]
	
	// Append the rest of the query without the LIMIT clause
	remaining := sql[limitIdx+len(" LIMIT ")+len(limitValue):]
	newSQL += remaining
	
	return newSQL
}

// convertDateTimeFunctions handles differences in date/time functions
func (e *ExternalSQLTranslator) convertDateTimeFunctions(sql, sourceDialect, targetDialect string) string {
	result := sql
	
	// Example: CONVERT(NOW(), DATE) in some dialects vs CURRENT_DATE in others
	if targetDialect == string(Oracle) {
		result = strings.ReplaceAll(result, "NOW()", "SYSDATE")
		result = strings.ReplaceAll(result, "CURRENT_DATE", "SYSDATE")
	} else if targetDialect == string(SQLServer) {
		result = strings.ReplaceAll(result, "CURRENT_DATE", "GETDATE()")
	}
	
	return result
}

// convertStringFunctions handles differences in string functions
func (e *ExternalSQLTranslator) convertStringFunctions(sql, sourceDialect, targetDialect string) string {
	result := sql
	
	// Example: Different functions for string concatenation or substring
	if targetDialect == string(Oracle) {
		result = strings.ReplaceAll(result, "CONCAT(", "CONCAT(") // Oracle has CONCAT but handles differently
		result = strings.ReplaceAll(result, "SUBSTRING(", "SUBSTR(")
	} else if targetDialect == string(SQLServer) {
		result = strings.ReplaceAll(result, "SUBSTRING(", "SUBSTRING(")
	}
	
	return result
}

// SupportedDialects returns a list of supported SQL dialects
func (e *ExternalSQLTranslator) SupportedDialects() []string {
	return e.supportedDialects
}

// Validate checks if the SQL is valid for the given dialect
func (e *ExternalSQLTranslator) Validate(sql, dialect string) (bool, error) {
	// In a real implementation, this would validate against the specific dialect
	// For now, we'll use the mock implementation
	mockTranslator := NewMockSQLTranslator()
	return mockTranslator.Validate(sql, dialect)
}

// GetTranslator returns the appropriate translator based on availability
func GetTranslator(useExternal bool) SQLTranslator {
	if useExternal {
		return NewExternalSQLTranslator(true)
	}
	return NewMockSQLTranslator()
}