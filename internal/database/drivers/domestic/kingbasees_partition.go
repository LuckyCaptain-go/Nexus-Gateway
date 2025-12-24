package domestic

import (
	"context"
	"database/sql"
	"fmt"
)

// KingbaseESPartitionManager handles KingbaseES partitioning features
type KingbaseESPartitionManager struct {
	driver *KingbaseESDriver
}

// NewKingbaseESPartitionManager creates a new partition manager
func NewKingbaseESPartitionManager(driver *KingbaseESDriver) *KingbaseESPartitionManager {
	return &KingbaseESPartitionManager{
		driver: driver,
	}
}

// CreateRangePartition creates a range partition
func (m *KingbaseESPartitionManager) CreateRangePartition(ctx context.Context, db *sql.DB, tableName, partitionName, startValue, endValue string) error {
	sql := fmt.Sprintf("ALTER TABLE %s ADD PARTITION %s VALUES LESS THAN ('%s')",
		tableName, partitionName, endValue)

	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to create range partition: %w", err)
	}
	return nil
}

// CreateListPartition creates a list partition
func (m *KingbaseESPartitionManager) CreateListPartition(ctx context.Context, db *sql.DB, tableName, partitionName string, values []string) error {
	valuesStr := fmt.Sprintf("('%s')", fmt.Sprintf("','", values))
	sql := fmt.Sprintf("ALTER TABLE %s ADD PARTITION %s VALUES IN %s",
		tableName, partitionName, valuesStr)

	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to create list partition: %w", err)
	}
	return nil
}

// CreateHashPartition creates a hash partition
func (m *KingbaseESPartitionManager) CreateHashPartition(ctx context.Context, db *sql.DB, tableName, partitionKey string, partitionCount int) error {
	sql := fmt.Sprintf("ALTER TABLE %s PARTITION BY HASH(%s) PARTITIONS %d",
		tableName, partitionKey, partitionCount)

	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to create hash partition: %w", err)
	}
	return nil
}

// DropPartition drops a partition
func (m *KingbaseESPartitionManager) DropPartition(ctx context.Context, db *sql.DB, tableName, partitionName string) error {
	sql := fmt.Sprintf("ALTER TABLE %s DROP PARTITION %s", tableName, partitionName)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to drop partition: %w", err)
	}
	return nil
}

// GetPartitionInfo retrieves partition information for a table
func (m *KingbaseESPartitionManager) GetPartitionInfo(ctx context.Context, db *sql.DB, tableName string) ([]KingbaseESPartitionInfo, error) {
	sql := `
		SELECT PARTITION_NAME, PARTITION_POSITION, PARTITION_METHOD,
		       HIGH_VALUE, TABLE_NAME
		FROM USER_TAB_PARTITIONS
		WHERE TABLE_NAME = ?
		ORDER BY PARTITION_POSITION
	`

	rows, err := db.QueryContext(ctx, sql, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to get partition info: %w", err)
	}
	defer rows.Close()

	var partitions []KingbaseESPartitionInfo

	for rows.Next() {
		var partition KingbaseESPartitionInfo
		err := rows.Scan(
			&partition.PartitionName,
			&partition.Position,
			&partition.Method,
			&partition.HighValue,
			&partition.TableName,
		)
		if err != nil {
			return nil, err
		}
		partitions = append(partitions, partition)
	}

	return partitions, nil
}

// KingbaseESPartitionInfo represents partition information
type KingbaseESPartitionInfo struct {
	PartitionName string
	Position      int
	Method        string // RANGE, LIST, HASH
	HighValue     string
	TableName     string
	Values        []string
}

// TruncatePartition truncates a partition
func (m *KingbaseESPartitionManager) TruncatePartition(ctx context.Context, db *sql.DB, tableName, partitionName string) error {
	sql := fmt.Sprintf("ALTER TABLE %s TRUNCATE PARTITION %s", tableName, partitionName)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to truncate partition: %w", err)
	}
	return nil
}

// SplitPartition splits a partition
func (m *KingbaseESPartitionManager) SplitPartition(ctx context.Context, db *sql.DB, tableName, partitionName, splitValue string) error {
	sql := fmt.Sprintf("ALTER TABLE %s SPLIT PARTITION %s AT ('%s') INTO (PARTITION %s_new, PARTITION %s_old)",
		tableName, partitionName, splitValue, partitionName, partitionName)

	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to split partition: %w", err)
	}
	return nil
}

// MergePartitions merges two partitions
func (m *KingbaseESPartitionManager) MergePartitions(ctx context.Context, db *sql.DB, tableName, partition1, partition2, mergedPartition string) error {
	sql := fmt.Sprintf("ALTER TABLE %s MERGE PARTITIONS %s AND %s INTO %s",
		tableName, partition1, partition2, mergedPartition)

	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to merge partitions: %w", err)
	}
	return nil
}

// ExchangePartition exchanges a partition with a table
func (m *KingbaseESPartitionManager) ExchangePartition(ctx context.Context, db *sql.DB, tableName, partitionName, targetTable string) error {
	sql := fmt.Sprintf("ALTER TABLE %s EXCHANGE PARTITION %s WITH TABLE %s",
		tableName, partitionName, targetTable)

	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to exchange partition: %w", err)
	}
	return nil
}

// GetPartitionStatistics retrieves statistics for a partition
func (m *KingbaseESPartitionManager) GetPartitionStatistics(ctx context.Context, db *sql.DB, tableName, partitionName string) (*KingbaseESPartitionStats, error) {
	sql := `
		SELECT NUM_ROWS, BLOCKS, AVG_ROW_LEN, LAST_ANALYZED
		FROM USER_TAB_PARTITIONS
		WHERE TABLE_NAME = ? AND PARTITION_NAME = ?
	`

	stats := &KingbaseESPartitionStats{
		TableName:     tableName,
		PartitionName: partitionName,
	}

	err := db.QueryRowContext(ctx, sql, tableName, partitionName).Scan(
		&stats.NumRows,
		&stats.Blocks,
		&stats.AvgRowLen,
		&stats.LastAnalyzed,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get partition statistics: %w", err)
	}

	return stats, nil
}

// KingbaseESPartitionStats represents partition statistics
type KingbaseESPartitionStats struct {
	TableName     string
	PartitionName string
	NumRows       int64
	Blocks        int64
	AvgRowLen     int
	LastAnalyzed  string
}
