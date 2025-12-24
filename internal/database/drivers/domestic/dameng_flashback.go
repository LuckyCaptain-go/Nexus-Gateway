package domestic

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// DaMengFlashbackHandler handles DaMeng flashback features
type DaMengFlashbackHandler struct {
	driver *DaMengDriver
}

// NewDaMengFlashbackHandler creates a new flashback handler
func NewDaMengFlashbackHandler(driver *DaMengDriver) *DaMengFlashbackHandler {
	return &DaMengFlashbackHandler{
		driver: driver,
	}
}

// QueryAtTimestamp queries data as of a specific timestamp
func (f *DaMengFlashbackHandler) QueryAtTimestamp(ctx context.Context, db *sql.DB, sql string, timestamp time.Time) (*DaMengQueryResult, error) {
	flashbackSQL := fmt.Sprintf("SELECT * FROM (%s) AS OF TIMESTAMP %s",
		sql,
		timestamp.Format("2006-01-02 15:04:05"))

	rows, err := db.QueryContext(ctx, flashbackSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to query at timestamp: %w", err)
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

// QueryAtSCN queries data as of a specific SCN (System Change Number)
func (f *DaMengFlashbackHandler) QueryAtSCN(ctx context.Context, db *sql.DB, sql string, scn int64) (*DaMengQueryResult, error) {
	flashbackSQL := fmt.Sprintf("SELECT * FROM (%s) AS OF SCN %d", sql, scn)

	rows, err := db.QueryContext(ctx, flashbackSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to query at SCN: %w", err)
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

// FlashbackTable flashes back a table to a specific timestamp
func (f *DaMengFlashbackHandler) FlashbackTable(ctx context.Context, db *sql.DB, tableName string, timestamp time.Time) error {
	sql := fmt.Sprintf("FLASHBACK TABLE %s TO TIMESTAMP %s",
		tableName,
		timestamp.Format("2006-01-02 15:04:05"))

	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to flashback table: %w", err)
	}
	return nil
}

// FlashbackTableToSCN flashes back a table to a specific SCN
func (f *DaMengFlashbackHandler) FlashbackTableToSCN(ctx context.Context, db *sql.DB, tableName string, scn int64) error {
	sql := fmt.Sprintf("FLASHBACK TABLE %s TO SCN %d", tableName, scn)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to flashback table to SCN: %w", err)
	}
	return nil
}

// GetCurrentSCN retrieves the current SCN
func (f *DaMengFlashbackHandler) GetCurrentSCN(ctx context.Context, db *sql.DB) (int64, error) {
	var scn int64
	err := db.QueryRowContext(ctx, "SELECT CURRENT_SCN FROM V$DATABASE").Scan(&scn)
	if err != nil {
		return 0, fmt.Errorf("failed to get current SCN: %w", err)
	}
	return scn, nil
}

// GetTableSCN retrieves the current SCN for a table
func (f *DaMengFlashbackHandler) GetTableSCN(ctx context.Context, db *sql.DB, tableName string) (int64, error) {
	var scn int64
	sql := fmt.Sprintf("SELECT OBJECT_SCN FROM USER_OBJECTS WHERE OBJECT_NAME = '%s'", tableName)
	err := db.QueryRowContext(ctx, sql).Scan(&scn)
	if err != nil {
		return 0, fmt.Errorf("failed to get table SCN: %w", err)
	}
	return scn, nil
}

// EnableFlashback enables flashback for a table
func (f *DaMengFlashbackHandler) EnableFlashback(ctx context.Context, db *sql.DB, tableName string) error {
	sql := fmt.Sprintf("ALTER TABLE %s ENABLE FLASHBACK", tableName)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to enable flashback: %w", err)
	}
	return nil
}

// DisableFlashback disables flashback for a table
func (f *DaMengFlashbackHandler) DisableFlashback(ctx context.Context, db *sql.DB, tableName string) error {
	sql := fmt.Sprintf("ALTER TABLE %s DISABLE FLASHBACK", tableName)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to disable flashback: %w", err)
	}
	return nil
}

// GetFlashbackInfo retrieves flashback information for a table
func (f *DaMengFlashbackHandler) GetFlashbackInfo(ctx context.Context, db *sql.DB, tableName string) (*DaMengFlashbackInfo, error) {
	sql := `
		SELECT TABLE_NAME, FLASHBACK_ENABLED, OLDEST_FLASHBACK_SCN, OLDEST_FLASHBACK_TIME
		FROM USER_TABLES
		WHERE TABLE_NAME = ?
	`

	info := &DaMengFlashbackInfo{
		TableName: tableName,
	}

	err := db.QueryRowContext(ctx, sql, tableName).Scan(
		&info.TableName,
		&info.FlashbackEnabled,
		&info.OldestFlashbackSCN,
		&info.OldestFlashbackTime,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get flashback info: %w", err)
	}

	return info, nil
}

// DaMengFlashbackInfo represents flashback information
type DaMengFlashbackInfo struct {
	TableName           string
	FlashbackEnabled    bool
	OldestFlashbackSCN  int64
	OldestFlashbackTime time.Time
}
