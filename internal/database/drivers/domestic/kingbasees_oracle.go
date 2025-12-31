package domestic

import (
	"context"
	"database/sql"
	"fmt"
)

// KingbaseESOracleCompat provides Oracle compatibility features
type KingbaseESOracleCompat struct {
	driver *KingbaseESDriver
}

// NewKingbaseESOracleCompat creates a new Oracle compatibility handler
func NewKingbaseESOracleCompat(driver *KingbaseESDriver) *KingbaseESOracleCompat {
	return &KingbaseESOracleCompat{
		driver: driver,
	}
}

// EnableOracleMode enables Oracle compatibility mode
func (c *KingbaseESOracleCompat) EnableOracleMode(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, "SET compatible_mode = 'ORA'")
	if err != nil {
		return fmt.Errorf("failed to enable Oracle mode: %w", err)
	}
	return nil
}

// CreateSequence creates an Oracle-style sequence
func (c *KingbaseESOracleCompat) CreateSequence(ctx context.Context, db *sql.DB, sequenceName string, start, increment int64) error {
	sql := fmt.Sprintf(`
		CREATE SEQUENCE %s
		START WITH %d
		INCREMENT BY %d
		NOCACHE
		NOCYCLE
	`, sequenceName, start, increment)

	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to create sequence: %w", err)
	}
	return nil
}

// GetSequenceNextVal gets the next value from a sequence
func (c *KingbaseESOracleCompat) GetSequenceNextVal(ctx context.Context, db *sql.DB, sequenceName string) (int64, error) {
	var nextVal int64
	sql := fmt.Sprintf("SELECT NEXTVAL('%s')", sequenceName)
	err := db.QueryRowContext(ctx, sql).Scan(&nextVal)
	if err != nil {
		return 0, fmt.Errorf("failed to get sequence next value: %w", err)
	}
	return nextVal, nil
}

// CreateSynonym creates an Oracle-style synonym
func (c *KingbaseESOracleCompat) CreateSynonym(ctx context.Context, db *sql.DB, synonymName, objectName string) error {
	sql := fmt.Sprintf("CREATE SYNONYM %s FOR %s", synonymName, objectName)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to create synonym: %w", err)
	}
	return nil
}

// CreateDatabaseLink creates a database link
func (c *KingbaseESOracleCompat) CreateDatabaseLink(ctx context.Context, db *sql.DB, linkName, username, password, host string, port int) error {
	sql := fmt.Sprintf(`
		CREATE DATABASE LINK %s
		CONNECT TO %s IDENTIFIED BY '%s'
		USING '%s:%d'
	`, linkName, username, password, host, port)

	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to create database link: %w", err)
	}
	return nil
}

// QueryWithLink queries using a database link
func (c *KingbaseESOracleCompat) QueryWithLink(ctx context.Context, db *sql.DB, sql string, linkName string) (*KingbaseESQueryResult, error) {
	querySQL := sql + "@" + linkName
	rows, err := db.QueryContext(ctx, querySQL)
	if err != nil {
		return nil, fmt.Errorf("failed to query via dblink: %w", err)
	}
	defer rows.Close()

	columns, _ := rows.Columns()
	var results []map[string]interface{}

	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		rowMap := make(map[string]interface{})
		for i, col := range columns {
			rowMap[col] = values[i]
		}
		results = append(results, rowMap)
	}

	return &KingbaseESQueryResult{
		Rows:    results,
		Columns: columns,
		Count:   len(results),
	}, nil
}

// CreatePackage creates an Oracle-style package
func (c *KingbaseESOracleCompat) CreatePackage(ctx context.Context, db *sql.DB, packageName, spec string, body string) error {
	// Create package spec
	specSQL := fmt.Sprintf("CREATE PACKAGE %s AS %s END %s;", packageName, spec, packageName)
	_, err := db.ExecContext(ctx, specSQL)
	if err != nil {
		return fmt.Errorf("failed to create package spec: %w", err)
	}

	// Create package body
	bodySQL := fmt.Sprintf("CREATE PACKAGE BODY %s AS %s END %s;", packageName, body, packageName)
	_, err = db.ExecContext(ctx, bodySQL)
	if err != nil {
		return fmt.Errorf("failed to create package body: %w", err)
	}

	return nil
}

// ExecutePackageFunction executes a package function
func (c *KingbaseESOracleCompat) ExecutePackageFunction(ctx context.Context, db *sql.DB, packageName, funcName string, args []interface{}) (interface{}, error) {
	sql := fmt.Sprintf("SELECT %s.%s(%s)", packageName, funcName, formatArgs(args))
	var result interface{}
	err := db.QueryRowContext(ctx, sql).Scan(&result)
	if err != nil {
		return nil, fmt.Errorf("failed to execute package function: %w", err)
	}
	return result, nil
}

// formatArgs formats function arguments
func formatArgs(args []interface{}) string {
	result := ""
	for i, arg := range args {
		if i > 0 {
			result += ", "
		}
		result += formatValue(arg)
	}
	return result
}

// formatValue formats a single value
func formatValue(v interface{}) string {
	switch val := v.(type) {
	case string:
		return "'" + val + "'"
	default:
		return fmt.Sprintf("%v", v)
	}
}

// GetOracleCompatibilityInfo retrieves Oracle compatibility info
func (c *KingbaseESOracleCompat) GetOracleCompatibilityInfo(ctx context.Context, db *sql.DB) (*KingbaseESOracleInfo, error) {
	info := &KingbaseESOracleInfo{
		Mode: "ORA",
	}

	// Check if PL/SQL is enabled
	var plsqlEnabled bool
	err := db.QueryRowContext(ctx, "SHOW plsql.enabled").Scan(&plsqlEnabled)
	if err == nil {
		info.PLSQLSupported = plsqlEnabled
	}

	return info, nil
}

// KingbaseESOracleInfo represents Oracle compatibility information
type KingbaseESOracleInfo struct {
	Mode              string
	PLSQLSupported    bool
	SynonymsSupported bool
	DBLinksSupported  bool
}
