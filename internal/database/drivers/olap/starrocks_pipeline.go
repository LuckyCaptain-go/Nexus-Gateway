package olap

import (
	"context"
	"database/sql"
	"fmt"
)

// StarRocksPipelineEngine handles StarRocks pipeline execution
type StarRocksPipelineEngine struct {
	driver *StarRocksDriver
}

// NewStarRocksPipelineEngine creates a new pipeline engine handler
func NewStarRocksPipelineEngine(driver *StarRocksDriver) *StarRocksPipelineEngine {
	return &StarRocksPipelineEngine{
		driver: driver,
	}
}

// EnablePipelineEngine enables the pipeline engine
func (e *StarRocksPipelineEngine) EnablePipelineEngine(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, "SET enable_pipeline_engine = 'true'")
	if err != nil {
		return fmt.Errorf("failed to enable pipeline engine: %w", err)
	}
	return nil
}

// DisablePipelineEngine disables the pipeline engine
func (e *StarRocksPipelineEngine) DisablePipelineEngine(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, "SET enable_pipeline_engine = 'false'")
	if err != nil {
		return fmt.Errorf("failed to disable pipeline engine: %w", err)
	}
	return nil
}

// SetParallelExecInstance sets parallel execution instances
func (e *StarRocksPipelineEngine) SetParallelExecInstance(ctx context.Context, db *sql.DB, instances int) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf("SET parallel_fragment_exec_instance_num = %d", instances))
	if err != nil {
		return fmt.Errorf("failed to set parallel exec instances: %w", err)
	}
	return nil
}

// SetPipelineDop sets pipeline degree of parallelism
func (e *StarRocksPipelineEngine) SetPipelineDop(ctx context.Context, db *sql.DB, dop int) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf("SET pipeline_dop = %d", dop))
	if err != nil {
		return fmt.Errorf("failed to set pipeline dop: %w", err)
	}
	return nil
}

// ExecutePipelineQuery executes a query using pipeline engine
func (e *StarRocksPipelineEngine) ExecutePipelineQuery(ctx context.Context, db *sql.DB, sql string) (*StarRocksPipelineResult, error) {
	// Enable pipeline engine
	if err := e.EnablePipelineEngine(ctx, db); err != nil {
		return nil, err
	}

	rows, err := db.QueryContext(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to execute pipeline query: %w", err)
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

	return &StarRocksPipelineResult{
		Rows:         results,
		Columns:      columns,
		RowCount:     len(results),
		PipelineUsed: true,
	}, nil
}

// GetPipelineStatus retrieves pipeline execution status
func (e *StarRocksPipelineEngine) GetPipelineStatus(ctx context.Context, db *sql.DB) (*StarRocksPipelineStatus, error) {
	var enabled bool
	var dop int

	err := db.QueryRowContext(ctx, "SHOW VARIABLES LIKE 'enable_pipeline_engine'").Scan(nil, &enabled)
	if err != nil {
		return nil, fmt.Errorf("failed to get pipeline status: %w", err)
	}

	err = db.QueryRowContext(ctx, "SHOW VARIABLES LIKE 'pipeline_dop'").Scan(nil, &dop)
	if err != nil {
		dop = 0 // Default if not set
	}

	return &StarRocksPipelineStatus{
		Enabled:     enabled,
		PipelineDOP: dop,
	}, nil
}

// StarRocksPipelineStatus represents pipeline status
type StarRocksPipelineStatus struct {
	Enabled     bool
	PipelineDOP int
}

// SetPipelineWorkloadGroup sets workload group for pipeline
func (e *StarRocksPipelineEngine) SetPipelineWorkloadGroup(ctx context.Context, db *sql.DB, workloadGroup string) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf("SET workload_group = '%s'", workloadGroup))
	if err != nil {
		return fmt.Errorf("failed to set workload group: %w", err)
	}
	return nil
}

// GetResourceUsage retrieves resource usage for pipeline queries
func (e *StarRocksPipelineEngine) GetResourceUsage(ctx context.Context, db *sql.DB) (*StarRocksResourceUsage, error) {
	sql := `
		SELECT QUERY_ID, MEMORY_USAGE,
		       CPU_TIME, EXECUTION_TIME
		FROM information_schema.running_queries
		WHERE ENABLE_PIPELINE_ENGINE = true
		LIMIT 100
	`

	rows, err := db.QueryContext(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to get resource usage: %w", err)
	}
	defer rows.Close()

	usage := &StarRocksResourceUsage{
		RunningQueries: []StarRocksQueryInfo{},
	}

	for rows.Next() {
		var query StarRocksQueryInfo
		err := rows.Scan(
			&query.QueryID,
			&query.MemoryUsage,
			&query.CPUTime,
			&query.ExecutionTime,
		)
		if err != nil {
			return nil, err
		}
		usage.RunningQueries = append(usage.RunningQueries, query)
	}

	return usage, nil
}

// StarRocksResourceUsage represents resource usage
type StarRocksResourceUsage struct {
	RunningQueries []StarRocksQueryInfo
}

// StarRocksQueryInfo represents query information
type StarRocksQueryInfo struct {
	QueryID       string
	MemoryUsage   int64
	CPUTime       int64
	ExecutionTime int64
}
