package domestic

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// OceanBaseCompatibilityDetector detects OceanBase compatibility mode
type OceanBaseCompatibilityDetector struct {
}

// NewOceanBaseCompatibilityDetector creates a new compatibility detector
func NewOceanBaseCompatibilityDetector() *OceanBaseCompatibilityDetector {
	return &OceanBaseCompatibilityDetector{}
}

// DetectMode detects whether OceanBase is running in MySQL or Oracle mode
func (d *OceanBaseCompatibilityDetector) DetectMode(ctx context.Context, db *sql.DB) (OceanBaseCompatMode, error) {
	var version string
	err := db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version)
	if err != nil {
		return OceanBaseCompatModeUnknown, fmt.Errorf("failed to get version: %w", err)
	}

	return d.parseModeFromVersion(version)
}

// parseModeFromVersion parses compatibility mode from version string
func (d *OceanBaseCompatibilityDetector) parseModeFromVersion(version string) (OceanBaseCompatMode, error) {
	version = strings.ToLower(version)

	// Check for Oracle mode indicators
	if strings.Contains(version, "oracle") ||
		strings.Contains(version, "_pl") ||
		strings.Contains(version, "compatibility=oracle") {
		return OceanBaseCompatModeOracle, nil
	}

	// Default to MySQL mode
	if strings.Contains(version, "oceanbase") ||
		strings.Contains(version, "mysql") {
		return OceanBaseCompatModeMySQL, nil
	}

	return OceanBaseCompatModeUnknown, nil
}

// DetectFromSystemTable detects mode from system tables
func (d *OceanBaseCompatibilityDetector) DetectFromSystemTable(ctx context.Context, db *sql.DB) (OceanBaseCompatMode, error) {
	// Try Oracle mode detection
	_, err := db.QueryContext(ctx, "SELECT * FROM V$VERSION WHERE ROWNUM = 1")
	if err == nil {
		return OceanBaseCompatModeOracle, nil
	}

	// Try MySQL mode detection
	_, err = db.QueryContext(ctx, "SHOW VARIABLES LIKE 'version_comment'")
	if err == nil {
		return OceanBaseCompatModeMySQL, nil
	}

	return OceanBaseCompatModeUnknown, fmt.Errorf("unable to detect compatibility mode")
}

// DetectFromSQLSyntax detects mode by testing SQL syntax
func (d *OceanBaseCompatibilityDetector) DetectFromSQLSyntax(ctx context.Context, db *sql.DB) (OceanBaseCompatMode, error) {
	// Test Oracle syntax: dual table
	_, err := db.QueryContext(ctx, "SELECT 1 FROM DUAL")
	if err == nil {
		return OceanBaseCompatModeOracle, nil
	}

	// Test MySQL syntax: no dual required
	_, err = db.QueryContext(ctx, "SELECT 1")
	if err == nil {
		return OceanBaseCompatModeMySQL, nil
	}

	return OceanBaseCompatModeUnknown, fmt.Errorf("unable to detect compatibility mode from SQL syntax")
}

// GetCompatModeInfo returns detailed compatibility mode information
func (d *OceanBaseCompatibilityDetector) GetCompatModeInfo(ctx context.Context, db *sql.DB) (*OceanBaseCompatInfo, error) {
	mode, err := d.DetectMode(ctx, db)
	if err != nil {
		return nil, err
	}

	info := &OceanBaseCompatInfo{
		Mode: mode,
	}

	// Get mode-specific info
	switch mode {
	case OceanBaseCompatModeOracle:
		info.Dialect = "PL/SQL"
		info.Features = OracleModeFeatures()
	case OceanBaseCompatModeMySQL:
		info.Dialect = "MySQL"
		info.Features = MySQLModeFeatures()
	default:
		info.Dialect = "Unknown"
		info.Features = []string{}
	}

	return info, nil
}

// OceanBaseCompatMode represents compatibility mode
type OceanBaseCompatMode int

const (
	OceanBaseCompatModeUnknown OceanBaseCompatMode = iota
	OceanBaseCompatModeMySQL
	OceanBaseCompatModeOracle
)

func (m OceanBaseCompatMode) String() string {
	switch m {
	case OceanBaseCompatModeMySQL:
		return "MYSQL"
	case OceanBaseCompatModeOracle:
		return "ORACLE"
	default:
		return "UNKNOWN"
	}
}

// OceanBaseCompatInfo holds compatibility mode information
type OceanBaseCompatInfo struct {
	Mode     OceanBaseCompatMode
	Dialect  string
	Features []string
}

// OracleModeFeatures returns Oracle mode features
func OracleModeFeatures() []string {
	return []string{
		"PL/SQL support",
		"Synonyms",
		"Sequences",
		"Packages",
		"Triggers",
		"Views WITH CHECK OPTION",
		"Materialized views",
		"DB links",
		"Flashback queries",
		"Partitioned tables",
	}
}

// MySQLModeFeatures returns MySQL mode features
func MySQLModeFeatures() []string {
	return []string{
		"MySQL protocol compatibility",
		"MySQL SQL syntax",
		"MySQL data types",
		"MySQL storage engines",
		"Replication support",
		"Sharding support",
	}
}

// IsMySQLMode checks if running in MySQL mode
func (i *OceanBaseCompatInfo) IsMySQLMode() bool {
	return i.Mode == OceanBaseCompatModeMySQL
}

// IsOracleMode checks if running in Oracle mode
func (i *OceanBaseCompatInfo) IsOracleMode() bool {
	return i.Mode == OceanBaseCompatModeOracle
}

// HasFeature checks if a specific feature is supported
func (i *OceanBaseCompatInfo) HasFeature(feature string) bool {
	for _, f := range i.Features {
		if strings.EqualFold(f, feature) {
			return true
		}
	}
	return false
}

// GetSQLDialect returns the SQL dialect string
func (i *OceanBaseCompatInfo) GetSQLDialect() string {
	return i.Dialect
}
