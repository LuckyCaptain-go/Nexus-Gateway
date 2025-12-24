package domestic

import (
	"context"
	"database/sql"
	"fmt"
)

// TiDBQueryOptimizer provides TiDB-specific query optimizations
type TiDBQueryOptimizer struct {
	driver *TiDBDriver
}

// NewTiDBQueryOptimizer creates a new query optimizer
func NewTiDBQueryOptimizer(driver *TiDBDriver) *TiDBQueryOptimizer {
	return &TiDBQueryOptimizer{
		driver: driver,
	}
}

// SetReplicaRead sets replica read preference
func (o *TiDBQueryOptimizer) SetReplicaRead(ctx context.Context, db *sql.DB, replicaPreference string) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf("SET tidb_replica_read = '%s'", replicaPreference))
	return err
}

// EnableOptimization enables specific optimizations
func (o *TiDBQueryOptimizer) EnableOptimization(ctx context.Context, db *sql.DB, optimization string) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf("SET tidb_%s = 1", optimization))
	return err
}

// GetQueryPlan retrieves query execution plan
func (o *TiDBQueryOptimizer) GetQueryPlan(ctx context.Context, db *sql.DB, sql string) (*TiDBExecutionPlan, error) {
	planSQL := "EXPLAIN FORMAT = JSON " + sql

	var planJSON string
	err := db.QueryRowContext(ctx, planSQL).Scan(&planJSON)
	if err != nil {
		return nil, fmt.Errorf("failed to get query plan: %w", err)
	}

	return &TiDBExecutionPlan{
		PlanJSON: planJSON,
	}, nil
}

// TiDBExecutionPlan represents execution plan
type TiDBExecutionPlan struct {
	PlanJSON string
	Tasks    []TiDBTaskInfo
}

// TiDBTaskInfo represents task information
type TiDBTaskInfo struct {
	TaskType string // cop, root, etc.
	EstRows  float64
}

// GetRegionInfo retrieves region information for a table
func (o *TiDBQueryOptimizer) GetRegionInfo(ctx context.Context, db *sql.DB, tableName string) (*TiDBRegionInfo, error) {
	sql := `
		SELECT TABLE_NAME, REGION_COUNT,
		       APPROXIMATE_KEYS, APPROXIMATE_SIZE
		FROM information_schema.TIDB_TABLES
		WHERE TABLE_NAME = ?
	`

	info := &TiDBRegionInfo{
		TableName: tableName,
	}

	err := db.QueryRowContext(ctx, sql, tableName).Scan(
		&info.TableName,
		&info.RegionCount,
		&info.ApproximateKeys,
		&info.ApproximateSize,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get region info: %w", err)
	}

	return info, nil
}

// TiDBRegionInfo represents region information
type TiDBRegionInfo struct {
	TableName       string
	RegionCount     int
	ApproximateKeys int64
	ApproximateSize int64
}

// GetHotSpotInfo retrieves hotspot information
func (o *TiDBQueryOptimizer) GetHotSpotInfo(ctx context.Context, db *sql.DB) ([]TiDBHotSpot, error) {
	sql := `
		SELECT TABLE_NAME, INDEX_NAME,
		       HOT_VALUE, HOT_TIME
		FROM information_schema.TIDB_HOTSPOT
		ORDER BY HOT_VALUE DESC
		LIMIT 100
	`

	rows, err := db.QueryContext(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to get hotspot info: %w", err)
	}
	defer rows.Close()

	var hotspots []TiDBHotSpot

	for rows.Next() {
		var hs TiDBHotSpot
		err := rows.Scan(
			&hs.TableName,
			&hs.IndexName,
			&hs.HotValue,
			&hs.HotTime,
		)
		if err != nil {
			return nil, err
		}
		hotspots = append(hotspots, hs)
	}

	return hotspots, nil
}

// TiDBHotSpot represents hotspot information
type TiDBHotSpot struct {
	TableName string
	IndexName string
	HotValue  float64
	HotTime   string
}
