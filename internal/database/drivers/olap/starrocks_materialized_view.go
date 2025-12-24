package olap

import (
	"context"
	"database/sql"
	"fmt"
)

// StarRocksMaterializedView handles StarRocks materialized views
type StarRocksMaterializedView struct {
	driver *StarRocksDriver
}

// NewStarRocksMaterializedView creates a new materialized view handler
func NewStarRocksMaterializedView(driver *StarRocksDriver) *StarRocksMaterializedView {
	return &StarRocksMaterializedView{
		driver: driver,
	}
}

// CreateMaterializedView creates a materialized view
func (m *StarRocksMaterializedView) CreateMaterializedView(ctx context.Context, db *sql.DB, mvName, query string) error {
	sql := fmt.Sprintf("CREATE MATERIALIZED VIEW %s AS %s", mvName, query)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to create materialized view: %w", err)
	}
	return nil
}

// RefreshMaterializedView refreshes a materialized view
func (m *StarRocksMaterializedView) RefreshMaterializedView(ctx context.Context, db *sql.DB, mvName string) error {
	sql := fmt.Sprintf("REFRESH MATERIALIZED VIEW %s", mvName)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to refresh materialized view: %w", err)
	}
	return nil
}

// DropMaterializedView drops a materialized view
func (m *StarRocksMaterializedView) DropMaterializedView(ctx context.Context, db *sql.DB, mvName string) error {
	sql := fmt.Sprintf("DROP MATERIALIZED VIEW %s", mvName)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to drop materialized view: %w", err)
	}
	return nil
}

// GetMaterializedViewInfo retrieves materialized view information
func (m *StarRocksMaterializedView) GetMaterializedViewInfo(ctx context.Context, db *sql.DB, mvName string) (*StarRocksMVInfo, error) {
	sql := `
		SELECT MV_NAME, TABLE_NAME, SQL_TEXT, IS_ACTIVE
		FROM information_schema.materialized_views
		WHERE MV_NAME = ?
	`

	info := &StarRocksMVInfo{
		MVName: mvName,
	}

	err := db.QueryRowContext(ctx, sql, mvName).Scan(
		&info.MVName,
		&info.TableName,
		&info.SQLText,
		&info.IsActive,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get materialized view info: %w", err)
	}

	return info, nil
}

// StarRocksMVInfo represents materialized view information
type StarRocksMVInfo struct {
	MVName    string
	TableName string
	SQLText   string
	IsActive  bool
	Rows      int64
}

// ListMaterializedViews lists all materialized views
func (m *StarRocksMaterializedView) ListMaterializedViews(ctx context.Context, db *sql.DB) ([]StarRocksMVInfo, error) {
	sql := `
		SELECT MV_NAME, TABLE_NAME, SQL_TEXT, IS_ACTIVE
		FROM information_schema.materialized_views
	`

	rows, err := db.QueryContext(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to list materialized views: %w", err)
	}
	defer rows.Close()

	var mvs []StarRocksMVInfo

	for rows.Next() {
		var mv StarRocksMVInfo
		err := rows.Scan(
			&mv.MVName,
			&mv.TableName,
			&mv.SQLText,
			&mv.IsActive,
		)
		if err != nil {
			return nil, err
		}
		mvs = append(mvs, mv)
	}

	return mvs, nil
}

// EnableAutoRefresh enables automatic refresh
func (m *StarRocksMaterializedView) EnableAutoRefresh(ctx context.Context, db *sql.DB, mvName string, interval string) error {
	sql := fmt.Sprintf("ALTER MATERIALIZED VIEW %s SET (REFRESH_INTERVAL = '%s')", mvName, interval)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to enable auto refresh: %w", err)
	}
	return nil
}

// DisableMaterializedView disables a materialized view
func (m *StarRocksMaterializedView) DisableMaterializedView(ctx context.Context, db *sql.DB, mvName string) error {
	sql := fmt.Sprintf("ALTER MATERIALIZED VIEW %s DISABLE", mvName)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to disable materialized view: %w", err)
	}
	return nil
}

// EnableMaterializedView enables a materialized view
func (m *StarRocksMaterializedView) EnableMaterializedView(ctx context.Context, db *sql.DB, mvName string) error {
	sql := fmt.Sprintf("ALTER MATERIALIZED VIEW %s ENABLE", mvName)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to enable materialized view: %w", err)
	}
	return nil
}
