package olap

import (
	"context"
	"database/sql"
	"fmt"
)

// DorisQueryOptimizer provides Doris-specific query optimizations
type DorisQueryOptimizer struct {
	driver *DorisDriver
}

// NewDorisQueryOptimizer creates a new query optimizer
func NewDorisQueryOptimizer(driver *DorisDriver) *DorisQueryOptimizer {
	return &DorisQueryOptimizer{
		driver: driver,
	}
}

// OptimizeWithRollup optimizes query using materialized views/rollups
func (o *DorisQueryOptimizer) OptimizeWithRollup(ctx context.Context, db *sql.DB, sql string, rollupTable string) (*DorisOptimizedResult, error) {
	// Set session variable to use specific rollup
	_, err := db.ExecContext(ctx, fmt.Sprintf("SET use_rollup = %s", rollupTable))
	if err != nil {
		return nil, fmt.Errorf("failed to set rollup: %w", err)
	}

	rows, err := db.QueryContext(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to execute optimized query: %w", err)
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

	return &DorisOptimizedResult{
		Rows:         results,
		Columns:      columns,
		Count:        len(results),
		Optimization: "rollup",
		RollupTable:  rollupTable,
	}, nil
}

// DorisOptimizedResult represents optimized query result
type DorisOptimizedResult struct {
	Rows         []map[string]interface{}
	Columns      []string
	Count        int
	Optimization string
	RollupTable  string
}

// SetParallelExec sets parallel execution options
func (o *DorisQueryOptimizer) SetParallelExec(ctx context.Context, db *sql.DB, parallelInstance int) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf("SET parallel_fragment_exec_instance_num = %d", parallelInstance))
	return err
}

// EnablePipeline enables pipeline engine
func (o *DorisQueryOptimizer) EnablePipeline(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, "SET enable_pipeline_engine = true")
	return err
}

// SetMemLimit sets memory limit for query
func (o *DorisQueryOptimizer) SetMemLimit(ctx context.Context, db *sql.DB, memLimit string) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf("SET exec_mem_limit = %s", memLimit))
	return err
}

// SetQueryTimeout sets query timeout
func (o *DorisQueryOptimizer) SetQueryTimeout(ctx context.Context, db *sql.DB, timeout int) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf("SET query_timeout = %d", timeout))
	return err
}

// GetQueryProfile retrieves query execution profile
func (o *DorisQueryOptimizer) GetQueryProfile(ctx context.Context, db *sql.DB, queryID string) (*DorisQueryProfile, error) {
	sql := `
		SELECT QUERY_ID, ELAPSED_TIME, CPU_TIME,
		       MEMORY_USAGE, ROWS_READ, ROWS_PRODUCED
		FROM information_schema.query_profile
		WHERE QUERY_ID = ?
	`

	profile := &DorisQueryProfile{
		QueryID: queryID,
	}

	err := db.QueryRowContext(ctx, sql, queryID).Scan(
		&profile.QueryID,
		&profile.ElapsedTime,
		&profile.CPUTime,
		&profile.MemoryUsage,
		&profile.RowsRead,
		&profile.RowsProduced,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get query profile: %w", err)
	}

	return profile, nil
}

// DorisQueryProfile represents query execution profile
type DorisQueryProfile struct {
	QueryID      string
	ElapsedTime  int64
	CPUTime      int64
	MemoryUsage  int64
	RowsRead     int64
	RowsProduced int64
}

// GetTabletDistribution retrieves tablet distribution
func (o *DorisQueryOptimizer) GetTabletDistribution(ctx context.Context, db *sql.DB, tableName string) (*DorisTabletDistribution, error) {
	sql := `
		SELECT TABLE_NAME, TABLET_COUNT,
		       REPLICA_COUNT, STORAGE_MEDIUM
		FROM information_schema.TABLES
		WHERE TABLE_NAME = ?
	`

	dist := &DorisTabletDistribution{
		TableName: tableName,
	}

	err := db.QueryRowContext(ctx, sql, tableName).Scan(
		&dist.TableName,
		&dist.TabletCount,
		&dist.ReplicaCount,
		&dist.StorageMedium,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get tablet distribution: %w", err)
	}

	return dist, nil
}

// DorisTabletDistribution represents tablet distribution
type DorisTabletDistribution struct {
	TableName      string
	TabletCount    int
	ReplicaCount   int
	StorageMedium  string // SSD, HDD
}
