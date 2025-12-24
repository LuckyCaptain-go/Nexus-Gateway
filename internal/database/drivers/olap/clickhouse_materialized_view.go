package olap

import (
	"context"
	"database/sql"
	"fmt"
)

// ClickHouseMaterializedView handles ClickHouse materialized views
type ClickHouseMaterializedView struct {
	driver *ClickHouseDriver
}

// NewClickHouseMaterializedView creates a new materialized view handler
func NewClickHouseMaterializedView(driver *ClickHouseDriver) *ClickHouseMaterializedView {
	return &ClickHouseMaterializedView{
		driver: driver,
	}
}

// CreateMaterializedView creates a materialized view
func (m *ClickHouseMaterializedView) CreateMaterializedView(ctx context.Context, db *sql.DB, mvName, targetTable, query string) error {
	sql := fmt.Sprintf("CREATE MATERIALIZED VIEW %s TO %s AS %s", mvName, targetTable, query)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to create materialized view: %w", err)
	}
	return nil
}

// DropMaterializedView drops a materialized view
func (m *ClickHouseMaterializedView) DropMaterializedView(ctx context.Context, db *sql.DB, mvName string) error {
	sql := fmt.Sprintf("DROP MATERIALIZED VIEW %s", mvName)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to drop materialized view: %w", err)
	}
	return nil
}

// GetMaterializedViewInfo retrieves materialized view information
func (m *ClickHouseMaterializedView) GetMaterializedViewInfo(ctx context.Context, db *sql.DB, mvName string) (*ClickHouseMVInfo, error) {
	sql := `
		SELECT name, target_table, query
		FROM system.materialized_views
		WHERE name = ?
	`

	info := &ClickHouseMVInfo{
		Name: mvName,
	}

	err := db.QueryRowContext(ctx, sql, mvName).Scan(
		&info.Name,
		&info.TargetTable,
		&info.Query,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get materialized view info: %w", err)
	}

	return info, nil
}

// ClickHouseMVInfo represents materialized view information
type ClickHouseMVInfo struct {
	Name        string
	TargetTable string
	Query       string
	Rows        int64
	Bytes       int64
}

// RefreshMaterializedView refreshes a materialized view
func (m *ClickHouseMaterializedView) RefreshMaterializedView(ctx context.Context, db *sql.DB, mvName string) error {
	// ClickHouse materialized views are updated automatically
	// This is a placeholder for any manual optimization needed
	return nil
}

// ListMaterializedViews lists all materialized views
func (m *ClickHouseMaterializedView) ListMaterializedViews(ctx context.Context, db *sql.DB) ([]ClickHouseMVInfo, error) {
	sql := `
		SELECT name, target_table, query
		FROM system.materialized_views
	`

	rows, err := db.QueryContext(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to list materialized views: %w", err)
	}
	defer rows.Close()

	var mvs []ClickHouseMVInfo

	for rows.Next() {
		var mv ClickHouseMVInfo
		err := rows.Scan(
			&mv.Name,
			&mv.TargetTable,
			&mv.Query,
		)
		if err != nil {
			return nil, err
		}
		mvs = append(mvs, mv)
	}

	return mvs, nil
}

// GetMVSize retrieves the size of a materialized view
func (m *ClickHouseMaterializedView) GetMVSize(ctx context.Context, db *sql.DB, mvName string) (int64, error) {
	sql := fmt.Sprintf("SELECT COUNT(*) FROM %s", mvName)
	var count int64
	err := db.QueryRowContext(ctx, sql).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get MV size: %w", err)
	}
	return count, nil
}
