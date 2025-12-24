package olap

import (
	"context"
	"database/sql"
	"fmt"
)

// DorisRollupManager handles Doris rollup features
type DorisRollupManager struct {
	driver *DorisDriver
}

// NewDorisRollupManager creates a new rollup manager
func NewDorisRollupManager(driver *DorisDriver) *DorisRollupManager {
	return &DorisRollupManager{
		driver: driver,
	}
}

// CreateRollup creates a rollup index
func (m *DorisRollupManager) CreateRollup(ctx context.Context, db *sql.DB, tableName, rollupName string, columns []string) error {
	columnsStr := fmt.Sprintf("(`%s`)", fmt.Sprintf("`,`", columns))
	sql := fmt.Sprintf("ALTER TABLE %s ADD ROLLUP %s %s", tableName, rollupName, columnsStr)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to create rollup: %w", err)
	}
	return nil
}

// DropRollup drops a rollup index
func (m *DorisRollupManager) DropRollup(ctx context.Context, db *sql.DB, tableName, rollupName string) error {
	sql := fmt.Sprintf("ALTER TABLE %s DROP ROLLUP %s", tableName, rollupName)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to drop rollup: %w", err)
	}
	return nil
}

// ListRollups lists all rollups for a table
func (m *DorisRollupManager) ListRollups(ctx context.Context, db *sql.DB, tableName string) ([]DorisRollupInfo, error) {
	sql := `
		SELECT TABLE_NAME, ROLLUP_NAME, COLUMN_NAME, ROW_COUNT
		FROM information_schema.ROLLUP_INDEXES
		WHERE TABLE_NAME = ?
	`

	rows, err := db.QueryContext(ctx, sql, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to list rollups: %w", err)
	}
	defer rows.Close()

	var rollups []DorisRollupInfo

	for rows.Next() {
		var rollup DorisRollupInfo
		err := rows.Scan(
			&rollup.TableName,
			&rollup.RollupName,
			&rollup.ColumnName,
			&rollup.RowCount,
		)
		if err != nil {
			return nil, err
		}
		rollups = append(rollups, rollup)
	}

	return rollups, nil
}

// DorisRollupInfo represents rollup information
type DorisRollupInfo struct {
	TableName  string
	RollupName string
	ColumnName string
	RowCount   int64
	IsVisible  bool
}

// GetRollupStatus retrieves rollup status
func (m *DorisRollupManager) GetRollupStatus(ctx context.Context, db *sql.DB, tableName, rollupName string) (*DorisRollupStatus, error) {
	sql := `
		SELECT STATE, PROGRESS, ROWS_TOTAL, ROWS_LOADED
		FROM information_schema.ROLLUP_JOBS
		WHERE TABLE_NAME = ? AND ROLLUP_NAME = ?
	`

	status := &DorisRollupStatus{
		TableName:  tableName,
		RollupName: rollupName,
	}

	err := db.QueryRowContext(ctx, sql, tableName, rollupName).Scan(
		&status.State,
		&status.Progress,
		&status.RowsTotal,
		&status.RowsLoaded,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get rollup status: %w", err)
	}

	return status, nil
}

// DorisRollupStatus represents rollup status
type DorisRollupStatus struct {
	TableName   string
	RollupName  string
	State       string // PENDING, LOADING, FINISHED, CANCELLED
	Progress    int
	RowsTotal   int64
	RowsLoaded  int64
}

// RebuildRollup rebuilds a rollup index
func (m *DorisRollupManager) RebuildRollup(ctx context.Context, db *sql.DB, tableName, rollupName string) error {
	sql := fmt.Sprintf("ALTER TABLE %s REBUILD ROLLUP %s", tableName, rollupName)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to rebuild rollup: %w", err)
	}
	return nil
}

// AddRollupColumn adds a column to an existing rollup
func (m *DorisRollupManager) AddRollupColumn(ctx context.Context, db *sql.DB, tableName, rollupName, columnName string) error {
	sql := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s TO ROLLUP %s", tableName, columnName, rollupName)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to add rollup column: %w", err)
	}
	return nil
}

// GetRollupProperties retrieves properties of a rollup
func (m *DorisRollupManager) GetRollupProperties(ctx context.Context, db *sql.DB, tableName, rollupName string) (map[string]string, error) {
	sql := `
		SELECT PROPERTY_KEY, PROPERTY_VALUE
		FROM information_schema.ROLLUP_PROPERTIES
		WHERE TABLE_NAME = ? AND ROLLUP_NAME = ?
	`

	rows, err := db.QueryContext(ctx, sql, tableName, rollupName)
	if err != nil {
		return nil, fmt.Errorf("failed to get rollup properties: %w", err)
	}
	defer rows.Close()

	properties := make(map[string]string)

	for rows.Next() {
		var key, value string
		err := rows.Scan(&key, &value)
		if err != nil {
			return nil, err
		}
		properties[key] = value
	}

	return properties, nil
}

// SetRollupProperty sets a property on a rollup
func (m *DorisRollupManager) SetRollupProperty(ctx context.Context, db *sql.DB, tableName, rollupName, key, value string) error {
	sql := fmt.Sprintf("ALTER TABLE %s MODIFY ROLLUP %s SET ('%s' = '%s')", tableName, rollupName, key, value)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to set rollup property: %w", err)
	}
	return nil
}
