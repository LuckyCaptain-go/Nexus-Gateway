package olap

import (
	"context"
	"database/sql"
	"fmt"
)

// DorisMaterializedView handles Doris materialized views
type DorisMaterializedView struct {
	driver *DorisDriver
}

// NewDorisMaterializedView creates a new materialized view handler
func NewDorisMaterializedView(driver *DorisDriver) *DorisMaterializedView {
	return &DorisMaterializedView{
		driver: driver,
	}
}

// CreateMaterializedView creates a materialized view
func (m *DorisMaterializedView) CreateMaterializedView(ctx context.Context, db *sql.DB, mvName, query string) error {
	sql := fmt.Sprintf("CREATE MATERIALIZED VIEW %s AS %s", mvName, query)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to create materialized view: %w", err)
	}
	return nil
}

// RefreshMaterializedView refreshes a materialized view
func (m *DorisMaterializedView) RefreshMaterializedView(ctx context.Context, db *sql.DB, mvName string) error {
	sql := fmt.Sprintf("REFRESH MATERIALIZED VIEW %s", mvName)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to refresh materialized view: %w", err)
	}
	return nil
}

// DropMaterializedView drops a materialized view
func (m *DorisMaterializedView) DropMaterializedView(ctx context.Context, db *sql.DB, mvName string) error {
	sql := fmt.Sprintf("DROP MATERIALIZED VIEW %s", mvName)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to drop materialized view: %w", err)
	}
	return nil
}

// GetMaterializedViewInfo retrieves materialized view information
func (m *DorisMaterializedView) GetMaterializedViewInfo(ctx context.Context, db *sql.DB, mvName string) (*DorisMVInfo, error) {
	sql := `
		SELECT MV_NAME, TABLE_NAME, SQL_TEXT
		FROM information_schema.materialized_views
		WHERE MV_NAME = ?
	`

	info := &DorisMVInfo{
		MVName: mvName,
	}

	err := db.QueryRowContext(ctx, sql, mvName).Scan(
		&info.MVName,
		&info.TableName,
		&info.SQLText,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get materialized view info: %w", err)
	}

	return info, nil
}

// DorisMVInfo represents materialized view information
type DorisMVInfo struct {
	MVName    string
	TableName string
	SQLText   string
	Rows      int64
	IsActive  bool
}

// ListMaterializedViews lists all materialized views
func (m *DorisMaterializedView) ListMaterializedViews(ctx context.Context, db *sql.DB) ([]DorisMVInfo, error) {
	sql := `
		SELECT MV_NAME, TABLE_NAME, SQL_TEXT
		FROM information_schema.materialized_views
	`

	rows, err := db.QueryContext(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to list materialized views: %w", err)
	}
	defer rows.Close()

	var mvs []DorisMVInfo

	for rows.Next() {
		var mv DorisMVInfo
		err := rows.Scan(
			&mv.MVName,
			&mv.TableName,
			&mv.SQLText,
		)
		if err != nil {
			return nil, err
		}
		mvs = append(mvs, mv)
	}

	return mvs, nil
}

// EnableAutoRefresh enables automatic refresh
func (m *DorisMaterializedView) EnableAutoRefresh(ctx context.Context, db *sql.DB, mvName string, interval string) error {
	sql := fmt.Sprintf("ALTER MATERIALIZED VIEW %s SET (REFRESH_INTERVAL = '%s')", mvName, interval)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to enable auto refresh: %w", err)
	}
	return nil
}
