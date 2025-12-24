package security

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode"

	"nexus-gateway/internal/model"
	"vitess.io/vitess/go/vt/sqlparser"
)

var (
	ErrNotSelectQuery     = errors.New("only SELECT queries are allowed")
	ErrSQLSyntaxError     = errors.New("SQL syntax error")
	ErrDangerousKeyword   = errors.New("dangerous SQL keyword detected")
	ErrSQLInjection       = errors.New("potential SQL injection detected")
	ErrEmptyQuery         = errors.New("query cannot be empty")
	ErrQueryTooLong       = errors.New("query exceeds maximum length")
)

// SQLValidator validates SQL queries for security
type SQLValidator struct {
	maxQueryLength int
}

// NewSQLValidator creates a new SQLValidator instance
func NewSQLValidator(maxQueryLength int) *SQLValidator {
	if maxQueryLength <= 0 {
		maxQueryLength = 10000 // Default max query length
	}
	return &SQLValidator{
		maxQueryLength: maxQueryLength,
	}
}

// ValidateStatement validates that the SQL statement is safe and read-only
func (sv *SQLValidator) ValidateStatement(sql string) error {
	// Basic checks
	if err := sv.basicValidation(sql); err != nil {
		return err
	}

	// Normalize SQL first (removes trailing semicolon and whitespace)
	normalizedSQL := sv.normalizeSQL(sql)

	// Parse SQL statement (use normalized SQL)
	stmt, err := sv.parseSQL(normalizedSQL)
	if err != nil {
		return ErrSQLSyntaxError
	}

	// Check if it's a SELECT statement
	if !sv.isSelectStatement(stmt) {
		return ErrNotSelectQuery
	}

	// Check for dangerous keywords (use normalized SQL)
	if err := sv.checkDangerousKeywords(normalizedSQL); err != nil {
		return err
	}

	// Check for potential SQL injection patterns (use normalized SQL)
	if err := sv.checkSQLInjection(normalizedSQL); err != nil {
		return err
	}

	return nil
}

// IsReadOnly checks if the SQL statement is read-only
func (sv *SQLValidator) IsReadOnly(sql string) (bool, error) {
	stmt, err := sv.parseSQL(sql)
	if err != nil {
		return false, ErrSQLSyntaxError
	}
	return sv.isSelectStatement(stmt), nil
}

// ParseSQL parses SQL statement using vitess parser
func (sv *SQLValidator) ParseSQL(sql string) (sqlparser.Statement, error) {
	return sv.parseSQL(sql)
}

// Private methods

func (sv *SQLValidator) basicValidation(sql string) error {
	// Check if query is empty
	if strings.TrimSpace(sql) == "" {
		return ErrEmptyQuery
	}

	// Check query length
	if len(sql) > sv.maxQueryLength {
		return ErrQueryTooLong
	}

	// Check for comment patterns that might hide malicious code
	if strings.Contains(sql, "--") || strings.Contains(sql, "/*") || strings.Contains(sql, "*/") {
		return ErrSQLInjection
	}

	return nil
}

func (sv *SQLValidator) parseSQL(sql string) (sqlparser.Statement, error) {
	// Normalize the SQL string
	normalizedSQL := sv.normalizeSQL(sql)

	// Parse using vitess SQL parser
	parser := sqlparser.NewTestParser()
	return parser.Parse(normalizedSQL)
}

func (sv *SQLValidator) isSelectStatement(stmt sqlparser.Statement) bool {
	switch stmt.(type) {
	case *sqlparser.Select:
		return true
	case *sqlparser.Union:
		return true
	default:
		return false
	}
}

func (sv *SQLValidator) checkDangerousKeywords(sql string) error {
	// List of dangerous keywords that should not be in SELECT queries
	dangerousKeywords := []string{
		"DROP", "DELETE", "UPDATE", "INSERT", "CREATE", "ALTER",
		"TRUNCATE", "REPLACE", "MERGE", "GRANT", "REVOKE",
		"COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE",
		"LOAD_FILE", "INTO OUTFILE", "INTO DUMPFILE",
		"EXEC", "EXECUTE", "CALL", "DO",
	}

	// Convert to uppercase for comparison
	upperSQL := strings.ToUpper(sql)

	for _, keyword := range dangerousKeywords {
		// Use word boundaries to avoid false positives
		pattern := `\b` + keyword + `\b`
		matched, _ := regexp.MatchString(pattern, upperSQL)
		if matched {
			return ErrDangerousKeyword
		}
	}

	return nil
}

func (sv *SQLValidator) checkSQLInjection(sql string) error {
	// Check for suspicious patterns
	// Note: sql should already be normalized (trailing semicolon removed)
	suspiciousPatterns := []string{
		// SQL comment patterns
		"--", "/*", "*/",
		// Multiple statements (check for multiple semicolons - should not exist after normalization)
		";.*;",  // Multiple semicolons indicate multiple statements
		";\\s+[a-z]",  // Content after semicolon (another statement)
		// UNION with SELECT in unusual places (potential injection)
		"union.*select.*union",
		// String concatenation patterns (multiple quotes in suspicious context)
		"'.*'.*'.*'",
		// Or patterns with suspicious operators (injection patterns)
		"\\bor\\s+1\\s*=\\s*1\\b",  // OR 1=1 (injection pattern)
		"\\band\\s+1\\s*=\\s*1\\b",  // AND 1=1 (injection pattern)
		// Common injection patterns
		"\\bor\\s+true\\b", "\\band\\s+true\\b",
		// Waitfor delays (common in injection testing)
		"waitfor\\s+delay",
		// Exec/Execute patterns (dangerous)
		"\\bexec\\s*\\(", "\\bexecute\\s*\\(",
		// Drop/Delete/Update/Insert patterns (should be caught by read-only check, but extra safety)
		"\\bdrop\\s+table\\b", "\\bdelete\\s+from\\b", "\\bupdate\\s+.*\\s+set\\b", "\\binsert\\s+into\\b",
	}

	lowerSQL := strings.ToLower(sql)

	for _, pattern := range suspiciousPatterns {
		matched, _ := regexp.MatchString(pattern, lowerSQL)
		if matched {
			return ErrSQLInjection
		}
	}

	// Check for unusual character sequences
	if sv.hasSuspiciousCharacters(sql) {
		return ErrSQLInjection
	}

	return nil
}

func (sv *SQLValidator) hasSuspiciousCharacters(sql string) bool {
	// Count non-printable characters
	suspiciousCount := 0
	for _, r := range sql {
		if !unicode.IsPrint(r) && !unicode.IsSpace(r) {
			suspiciousCount++
			if suspiciousCount > 5 {
				return true
			}
		}
	}

	// Check for repeated characters (might indicate obfuscation)
	// Only check for 3+ consecutive identical non-space characters
	if len(sql) > 10 {
		for i := 0; i < len(sql)-2; i++ {
			char := sql[i]
			// Skip spaces and common SQL characters that might repeat legitimately
			if char != ' ' && char != '\t' && char != '\n' && 
			   sql[i] == sql[i+1] && sql[i+1] == sql[i+2] {
				return true
			}
		}
	}

	return false
}

func (sv *SQLValidator) normalizeSQL(sql string) string {
	// Remove leading/trailing whitespace
	sql = strings.TrimSpace(sql)

	// Remove trailing semicolon if present
	if strings.HasSuffix(sql, ";") {
		sql = strings.TrimSuffix(sql, ";")
		sql = strings.TrimSpace(sql)
	}

	return sql
}

// GetTableName extracts table name from SELECT statement
func (sv *SQLValidator) GetTableName(sql string) (string, error) {
	stmt, err := sv.parseSQL(sql)
	if err != nil {
		return "", err
	}

	selectStmt, ok := stmt.(*sqlparser.Select)
	if !ok {
		return "", fmt.Errorf("not a SELECT statement")
	}

	// Extract table name from FROM clause
	if selectStmt.From != nil {
		if tableExpr, ok := selectStmt.From[0].(*sqlparser.AliasedTableExpr); ok {
			if tableName, ok := tableExpr.Expr.(*sqlparser.TableName); ok {
				return tableName.Name.String(), nil
			}
		}
	}

	return "", fmt.Errorf("table name not found")
}

// CheckQueryComplexity assesses query complexity
func (sv *SQLValidator) CheckQueryComplexity(sql string) (complexity int, err error) {
	stmt, err := sv.parseSQL(sql)
	if err != nil {
		return 0, err
	}

	switch s := stmt.(type) {
	case *sqlparser.Select:
		return sv.calculateSelectComplexity(s), nil
	case *sqlparser.Union:
		// Union queries are more complex
		var leftComplexity, rightComplexity int
		if leftSelect, ok := s.Left.(*sqlparser.Select); ok {
			leftComplexity = sv.calculateSelectComplexity(leftSelect)
		}
		if rightSelect, ok := s.Right.(*sqlparser.Select); ok {
			rightComplexity = sv.calculateSelectComplexity(rightSelect)
		}
		return leftComplexity + rightComplexity + 10, nil
	default:
		return 0, fmt.Errorf("unsupported statement type")
	}
}

func (sv *SQLValidator) calculateSelectComplexity(selectStmt *sqlparser.Select) int {
	complexity := 0

	// Base complexity
	complexity += 1

	// Check for JOINs
	if selectStmt.From != nil && len(selectStmt.From) > 1 {
		complexity += len(selectStmt.From) * 5
	}

	// Check for WHERE clause
	if selectStmt.Where != nil {
		complexity += 3
	}

	// Check for GROUP BY
	if selectStmt.GroupBy != nil {
		complexity += 2
	}

	// Check for HAVING
	if selectStmt.Having != nil {
		complexity += 2
	}

	// Check for ORDER BY
	if selectStmt.OrderBy != nil {
		complexity += 2
	}

	// Check for LIMIT
	if selectStmt.Limit != nil {
		complexity += 1
	}

	return complexity
}

// =============================================================================
// Data Source-Specific Validation (Phase 1 Extensions)
// =============================================================================

// DataSourceSQLConfig holds SQL dialect configuration for a data source type
type DataSourceSQLConfig struct {
	SupportsTimeTravel     bool
	SupportsWindowFunctions bool
	SupportsArrays          bool
	SupportsStructs         bool
	SupportsJSON            bool
	IdentifierQuote        string // e.g., `"` for PostgreSQL, "`" for MySQL
	TimeTravelSyntax       string // e.g., "FOR SYSTEM_TIME AS OF", "TABLE"
}

// GetDataSourceSQLConfig returns SQL configuration for a data source type
func GetDataSourceSQLConfig(dbType model.DatabaseType) DataSourceSQLConfig {
	switch dbType {
	// Data Lake Table Formats
	case model.DatabaseTypeApacheIceberg:
		return DataSourceSQLConfig{
			SupportsTimeTravel:      true,
			SupportsWindowFunctions: true,
			SupportsArrays:          true,
			SupportsStructs:         true,
			SupportsJSON:            true,
			IdentifierQuote:         `"`,
			TimeTravelSyntax:        "FOR SYSTEM_TIME AS OF",
		}
	case model.DatabaseTypeDeltaLake:
		return DataSourceSQLConfig{
			SupportsTimeTravel:      true,
			SupportsWindowFunctions: true,
			SupportsArrays:          true,
			SupportsStructs:         true,
			SupportsJSON:            true,
			IdentifierQuote:         "`",
			TimeTravelSyntax:        "VERSION AS OF",
		}
	case model.DatabaseTypeApacheHudi:
		return DataSourceSQLConfig{
			SupportsTimeTravel:      true,
			SupportsWindowFunctions: true,
			SupportsArrays:          true,
			SupportsStructs:         true,
			SupportsJSON:            true,
			IdentifierQuote:         "`",
			TimeTravelSyntax:        " TIMESTAMP AS OF",
		}

	// Cloud Warehouses
	case model.DatabaseTypeSnowflake:
		return DataSourceSQLConfig{
			SupportsTimeTravel:      true,
			SupportsWindowFunctions: true,
			SupportsArrays:          true,
			SupportsStructs:         true,
			SupportsJSON:            true,
			IdentifierQuote:         `"`,
			TimeTravelSyntax:        "AT",
		}
	case model.DatabaseTypeDatabricks:
		return DataSourceSQLConfig{
			SupportsTimeTravel:      true,
			SupportsWindowFunctions: true,
			SupportsArrays:          true,
			SupportsStructs:         true,
			SupportsJSON:            true,
			IdentifierQuote:         "`",
			TimeTravelSyntax:        "VERSION AS OF",
		}
	case model.DatabaseTypeBigQuery:
		return DataSourceSQLConfig{
			SupportsTimeTravel:      true,
			SupportsWindowFunctions: true,
			SupportsArrays:          true,
			SupportsStructs:         true,
			SupportsJSON:            true,
			IdentifierQuote:         "``,
			TimeTravelSyntax:        "FOR SYSTEM_TIME AS OF",
		}
	case model.DatabaseTypeRedshift:
		return DataSourceSQLConfig{
			SupportsTimeTravel:      false,
			SupportsWindowFunctions: true,
			SupportsArrays:          false,
			SupportsStructs:         false,
			SupportsJSON:            true,
			IdentifierQuote:         `"`,
			TimeTravelSyntax:        "",
		}

	// OLAP Engines
	case model.DatabaseTypeClickHouse:
		return DataSourceSQLConfig{
			SupportsTimeTravel:      false,
			SupportsWindowFunctions: true,
			SupportsArrays:          true,
			SupportsStructs:         true,
			SupportsJSON:            true,
			IdentifierQuote:         "``,
			TimeTravelSyntax:        "",
		}
	case model.DatabaseTypeApacheDoris, model.DatabaseTypeStarRocks, model.DatabaseTypeApacheDruid:
		return DataSourceSQLConfig{
			SupportsTimeTravel:      false,
			SupportsWindowFunctions: true,
			SupportsArrays:          false,
			SupportsStructs:         false,
			SupportsJSON:            true,
			IdentifierQuote:         "``,
			TimeTravelSyntax:        "",
		}

	// Object Storage - No SQL, API-based
	case model.DatabaseTypeS3, model.DatabaseTypeMinIO, model.DatabaseTypeAlibabaOSS,
		model.DatabaseTypeTencentCOS, model.DatabaseTypeAzureBlob:
		return DataSourceSQLConfig{
			SupportsTimeTravel:      false,
			SupportsWindowFunctions: false,
			SupportsArrays:          false,
			SupportsStructs:         false,
			SupportsJSON:            false,
			IdentifierQuote:         "",
			TimeTravelSyntax:        "",
		}

	// Domestic Databases
	case model.DatabaseTypeOceanBase, model.DatabaseTypeTiDB:
		return DataSourceSQLConfig{
			SupportsTimeTravel:      false,
			SupportsWindowFunctions: true,
			SupportsArrays:          false,
			SupportsStructs:         false,
			SupportsJSON:            true,
			IdentifierQuote:         "`",
			TimeTravelSyntax:        "",
		}

	// File Systems
	case model.DatabaseTypeHDFS, model.DatabaseTypeOzone:
		return DataSourceSQLConfig{
			SupportsTimeTravel:      false,
			SupportsWindowFunctions: false,
			SupportsArrays:          false,
			SupportsStructs:         false,
			SupportsJSON:            false,
			IdentifierQuote:         "",
			TimeTravelSyntax:        "",
		}

	// Relational Databases (existing)
	case model.DatabaseTypeMySQL, model.DatabaseTypeMariaDB:
		return DataSourceSQLConfig{
			SupportsTimeTravel:      false,
			SupportsWindowFunctions: true,
			SupportsArrays:          false,
			SupportsStructs:         false,
			SupportsJSON:            true,
			IdentifierQuote:         "`",
			TimeTravelSyntax:        "",
		}
	case model.DatabaseTypePostgreSQL:
		return DataSourceSQLConfig{
			SupportsTimeTravel:      false,
			SupportsWindowFunctions: true,
			SupportsArrays:          true,
			SupportsStructs:         false,
			SupportsJSON:            true,
			IdentifierQuote:         `"`,
			TimeTravelSyntax:        "",
		}
	case model.DatabaseTypeOracle:
		return DataSourceSQLConfig{
			SupportsTimeTravel:      false,
			SupportsWindowFunctions: true,
			SupportsArrays:          true,
			SupportsStructs:         true,
			SupportsJSON:            true,
			IdentifierQuote:         `"`,
			TimeTravelSyntax:        "",
		}

	default:
		// Default to MySQL-like configuration
		return DataSourceSQLConfig{
			SupportsTimeTravel:      false,
			SupportsWindowFunctions: true,
			SupportsArrays:          false,
			SupportsStructs:         false,
			SupportsJSON:            true,
			IdentifierQuote:         "`",
			TimeTravelSyntax:        "",
		}
	}
}

// ValidateDataSourceSyntax validates SQL syntax for a specific data source type
func (sv *SQLValidator) ValidateDataSourceSyntax(sql string, dbType model.DatabaseType) error {
	config := GetDataSourceSQLConfig(dbType)

	// Basic validation first
	if err := sv.ValidateStatement(sql); err != nil {
		return err
	}

	// Validate data source-specific syntax
	return sv.validateDataSourceSpecificSyntax(sql, dbType, config)
}

// validateDataSourceSpecificSyntax validates data source-specific SQL features
func (sv *SQLValidator) validateDataSourceSpecificSyntax(sql string, dbType model.DatabaseType, config DataSourceSQLConfig) error {
	sqlUpper := strings.ToUpper(sql)

	// Check for time travel syntax if not supported
	if config.TimeTravelSyntax == "" && sv.hasTimeTravelSyntax(sqlUpper) {
		return fmt.Errorf("time travel queries not supported for %s", dbType)
	}

	// Check for unsupported complex types
	if !config.SupportsArrays && sv.hasArraySyntax(sql) {
		return fmt.Errorf("array types not supported for %s", dbType)
	}

	if !config.SupportsStructs && sv.hasStructSyntax(sql) {
		return fmt.Errorf("struct types not supported for %s", dbType)
	}

	return nil
}

// hasTimeTravelSyntax checks if SQL contains time travel syntax
func (sv *SQLValidator) hasTimeTravelSyntax(sqlUpper string) bool {
	timeTravelPatterns := []string{
		"FOR SYSTEM_TIME AS OF",
		"VERSION AS OF",
		"TIMESTAMP AS OF",
		"AT TIMESTAMP",
		"FOR SYSTEM TIME",
	}

	for _, pattern := range timeTravelPatterns {
		if strings.Contains(sqlUpper, pattern) {
			return true
		}
	}

	return false
}

// hasArraySyntax checks if SQL contains array syntax
func (sv *SQLValidator) hasArraySyntax(sql string) bool {
	// Check for ARRAY type or bracket notation
	arrayPatterns := []string{
		"ARRAY[",
		"ARRAY<",
		"::ARRAY",
		"[", // Could be array subscript
	}

	upperSQL := strings.ToUpper(sql)
	for _, pattern := range arrayPatterns {
		if strings.Contains(upperSQL, pattern) || strings.Contains(sql, pattern) {
			return true
		}
	}

	return false
}

// hasStructSyntax checks if SQL contains struct syntax
func (sv *SQLValidator) hasStructSyntax(sql string) bool {
	structPatterns := []string{
		"STRUCT<",
		"ROW(",
		"::STRUCT",
	}

	upperSQL := strings.ToUpper(sql)
	for _, pattern := range structPatterns {
		if strings.Contains(upperSQL, pattern) {
			return true
		}
	}

	return false
}

// SupportsQueryType checks if a data source supports a specific query type
func (sv *SQLValidator) SupportsQueryType(dbType model.DatabaseType, queryType string) bool {
	config := GetDataSourceSQLConfig(dbType)

	switch strings.ToUpper(queryType) {
	case "TIMETRAVEL":
		return config.SupportsTimeTravel
	case "WINDOW":
		return config.SupportsWindowFunctions
	case "ARRAY":
		return config.SupportsArrays
	case "STRUCT":
		return config.SupportsStructs
	case "JSON":
		return config.SupportsJSON
	default:
		return true
	}
}

// SanitizeSQLForDataSource sanitizes SQL for a specific data source
func (sv *SQLValidator) SanitizeSQLForDataSource(sql string, dbType model.DatabaseType) (string, error) {
	config := GetDataSourceSQLConfig(dbType)

	// Validate first
	if err := sv.ValidateDataSourceSyntax(sql, dbType); err != nil {
		return "", err
	}

	// Normalize identifiers (quote them properly)
	sanitized := sv.normalizeIdentifiers(sql, config.IdentifierQuote)

	return sanitized, nil
}

// normalizeIdentifiers normalizes SQL identifiers for the data source
func (sv *SQLValidator) normalizeIdentifiers(sql, quoteChar string) string {
	if quoteChar == "" {
		return sql
	}

	// This is a simplified implementation
	// In production, you would use the SQL parser to properly identify and quote identifiers
	normalized := sql

	// Replace unquoted identifiers with quoted ones (simplified)
	// This is a placeholder - proper implementation requires AST parsing

	return normalized
}