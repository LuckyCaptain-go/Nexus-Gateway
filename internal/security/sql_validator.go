package security

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"unicode"

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