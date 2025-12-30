package domestic

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// DaMengProtocolClient handles DaMeng-specific protocol features
type DaMengProtocolClient struct {
	driver *DaMengDriver
}

// NewDaMengProtocolClient creates a new DaMeng protocol client
func NewDaMengProtocolClient(driver *DaMengDriver) *DaMengProtocolClient {
	return &DaMengProtocolClient{
		driver: driver,
	}
}

// ExecuteBatch executes batch operations
func (c *DaMengProtocolClient) ExecuteBatch(ctx context.Context, db *sql.DB, operations []DaMengBatchOperation) (*DaMengBatchResult, error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	result := &DaMengBatchResult{
		SuccessCount: 0,
		FailureCount: 0,
		Results:      make([]interface{}, len(operations)),
	}

	for i, op := range operations {
		var err error
		var res interface{}

		switch op.Type {
		case "INSERT":
			res, err = tx.ExecContext(ctx, op.SQL, op.Params...)
		case "UPDATE":
			res, err = tx.ExecContext(ctx, op.SQL, op.Params...)
		case "DELETE":
			res, err = tx.ExecContext(ctx, op.SQL, op.Params...)
		case "QUERY":
			res, err = tx.QueryContext(ctx, op.SQL, op.Params...)
		default:
			err = fmt.Errorf("unknown operation type: %s", op.Type)
		}

		if err != nil {
			result.FailureCount++
			result.Errors = append(result.Errors, fmt.Errorf("operation %d failed: %w", i, err))
			result.Results[i] = nil
		} else {
			result.SuccessCount++
			result.Results[i] = res
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return result, nil
}

// DaMengBatchOperation represents a batch operation
type DaMengBatchOperation struct {
	Type   string
	SQL    string
	Params []interface{}
}

// DaMengBatchResult represents batch operation results
type DaMengBatchResult struct {
	SuccessCount int
	FailureCount int
	Results      []interface{}
	Errors       []error
}

// ExecuteWithTableHint executes query with DaMeng table hints
func (c *DaMengProtocolClient) ExecuteWithTableHint(ctx context.Context, db *sql.DB, sql string, hints []string) (*DaMengQueryResult, error) {
	// DaMeng table hints syntax: /*+ HINT_NAME(table) */
	if len(hints) > 0 {
		hintStr := "/*+ " + strings.Join(hints, " ") + " */"
		sql = hintStr + " " + sql
	}

	rows, err := db.QueryContext(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to execute hinted query: %w", err)
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

	return &DaMengQueryResult{
		Rows:    results,
		Columns: columns,
		Count:   len(results),
	}, nil
}

// GetTableStatistics retrieves table statistics
func (c *DaMengProtocolClient) GetTableStatistics(ctx context.Context, db *sql.DB, tableName string) (*DaMengTableStats, error) {
	// NOTE: Original implementation used Oracle-style USER_TABLES.
	// DaMeng may expose different system views. Attempt to read basic info
	// from INFORMATION_SCHEMA.TABLES as a best-effort, but not all
	// columns (ROW_COUNT, BLOCKS, AVG_ROW_LEN, LAST_ANALYZED) are
	// standardized. Return what is available and leave others zeroed.

	query := `
		SELECT TABLE_NAME
		FROM INFORMATION_SCHEMA.TABLES
		WHERE TABLE_NAME = ?
	`

	stats := &DaMengTableStats{TableName: tableName}

	// Try to get a basic row to validate existence.
	var foundName string
	err := db.QueryRowContext(ctx, query, tableName).Scan(&foundName)
	if err != nil {
		// Fallback: try counting rows (may be expensive)
		countSQL := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
		if err2 := db.QueryRowContext(ctx, countSQL).Scan(&stats.RowCount); err2 != nil {
			return nil, fmt.Errorf("failed to get table statistics (info schema and count failed): %w / %v", err, err2)
		}
		return stats, nil
	}
	// If we found the table, leave other stats as zero (unknown)
	_ = foundName
	return stats, nil
}

// DaMengTableStats represents DaMeng table statistics
type DaMengTableStats struct {
	TableName    string
	RowCount     int64
	Blocks       int64
	AvgRowLen    int
	LastAnalyzed string
}

// ExplainQuery explains a query execution plan
func (c *DaMengProtocolClient) ExplainQuery(ctx context.Context, db *sql.DB, sql string) (*DaMengExplainPlan, error) {
	// Different DB engines expose explain plans differently. For DaMeng
	// try a generic EXPLAIN; return the textual plan as steps.

	explainSQL := "EXPLAIN " + sql
	rows, err := db.QueryContext(ctx, explainSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to run EXPLAIN: %w", err)
	}
	defer rows.Close()

	plan := &DaMengExplainPlan{Steps: []DaMengPlanStep{}}

	// Read textual explain output rows and store as Operation text.
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return nil, err
		}
		plan.Steps = append(plan.Steps, DaMengPlanStep{Operation: line})
	}

	return plan, nil
}

// DaMengExplainPlan represents execution plan
type DaMengExplainPlan struct {
	Steps []DaMengPlanStep
}

// DaMengPlanStep represents a plan step
type DaMengPlanStep struct {
	ID          int
	Operation   string
	Options     string
	ObjectName  string
	Cardinality int64
	Bytes       int64
	Cost        float64
}

// GetWaitEvents retrieves current wait events
func (c *DaMengProtocolClient) GetWaitEvents(ctx context.Context, db *sql.DB) ([]DaMengWaitEvent, error) {
	// V$SYSTEM_EVENT is an Oracle-style view. DaMeng may not expose it.
	// Return empty slice and let caller handle lack of wait-event visibility.
	return nil, fmt.Errorf("GetWaitEvents: not implemented for DaMeng — requires DB-specific system view")
}

// DaMengWaitEvent represents a wait event
type DaMengWaitEvent struct {
	EventName   string
	TotalWaits  int64
	TimeWaited  float64
	AverageWait float64
}

// GetTablespaces retrieves tablespace information
func (c *DaMengProtocolClient) GetTablespaces(ctx context.Context, db *sql.DB) ([]DaMengTablespace, error) {
	// USER_TABLESPACES is Oracle-specific. DaMeng tablespace info may
	// be exposed differently. Return not implemented to avoid incorrect queries.
	return nil, fmt.Errorf("GetTablespaces: not implemented for DaMeng — requires DB-specific system view")
}

// DaMengTablespace represents tablespace info
type DaMengTablespace struct {
	Name      string
	BlockSize int
	Status    string
	Contents  string
	Logging   string
}
