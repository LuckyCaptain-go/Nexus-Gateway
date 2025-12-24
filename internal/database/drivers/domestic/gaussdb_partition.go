package domestic

import (
	"context"
	"database/sql"
	"fmt"
)

// GaussDBPartitionManager handles GaussDB partitioning features
type GaussDBPartitionManager struct {
	driver *GaussDBDriver
}

// NewGaussDBPartitionManager creates a new partition manager
func NewGaussDBPartitionManager(driver *GaussDBDriver) *GaussDBPartitionManager {
	return &GaussDBPartitionManager{
		driver: driver,
	}
}

// CreateRangePartition creates a range partition
func (m *GaussDBPartitionManager) CreateRangePartition(ctx context.Context, db *sql.DB, tableName, partitionName, startValue, endValue string) error {
	sql := fmt.Sprintf("ALTER TABLE %s ADD PARTITION %s VALUES LESS THAN ('%s')",
		tableName, partitionName, endValue)

	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to create range partition: %w", err)
	}
	return nil
}

// GetPartitionInfo retrieves partition information for a table
func (m *GaussDBPartitionManager) GetPartitionInfo(ctx context.Context, db *sql.DB, tableName string) (*GaussDBPartitionInfo, error) {
	sql := `
		SELECT TABLE_NAME, PARTITION_NAME,
		       PARTITION_POSITION, PARTITION_KEY
		FROM USER_TAB_PARTITIONS
		WHERE TABLE_NAME = ?
	`

	info := &GaussDBPartitionInfo{
		TableName: tableName,
	}

	err := db.QueryRowContext(ctx, sql, tableName).Scan(
		&info.TableName,
		&info.PartitionName,
		&info.PartitionPosition,
		&info.PartitionKey,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get partition info: %w", err)
	}

	return info, nil
}

// GaussDBPartitionInfo represents partition information
type GaussDBPartitionInfo struct {
	TableName          string
	PartitionName      string
	PartitionPosition  int
	PartitionKey       string
	PartitionCount     int
}

// ListPartitions lists all partitions for a table
func (m *GaussDBPartitionManager) ListPartitions(ctx context.Context, db *sql.DB, tableName string) ([]GaussDBPartitionInfo, error) {
	sql := `
		SELECT TABLE_NAME, PARTITION_NAME,
		       PARTITION_POSITION, PARTITION_KEY
		FROM USER_TAB_PARTITIONS
		WHERE TABLE_NAME = ?
		ORDER BY PARTITION_POSITION
	`

	rows, err := db.QueryContext(ctx, sql, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to list partitions: %w", err)
	}
	defer rows.Close()

	var partitions []GaussDBPartitionInfo

	for rows.Next() {
		var partition GaussDBPartitionInfo
		err := rows.Scan(
			&partition.TableName,
			&partition.PartitionName,
			&partition.PartitionPosition,
			&partition.PartitionKey,
		)
		if err != nil {
			return nil, err
		}
		partitions = append(partitions, partition)
	}

	return partitions, nil
}

// DropPartition drops a partition
func (m *GaussDBPartitionManager) DropPartition(ctx context.Context, db *sql.DB, tableName, partitionName string) error {
	sql := fmt.Sprintf("ALTER TABLE %s DROP PARTITION %s", tableName, partitionName)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to drop partition: %w", err)
	}
	return nil
}

// TruncatePartition truncates a partition
func (m *GaussDBPartitionManager) TruncatePartition(ctx context.Context, db *sql.DB, tableName, partitionName string) error {
	sql := fmt.Sprintf("ALTER TABLE %s TRUNCATE PARTITION %s", tableName, partitionName)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to truncate partition: %w", err)
	}
	return nil
}

// SplitPartition splits a partition
func (m *GaussDBPartitionManager) SplitPartition(ctx context.Context, db *sql.DB, tableName, partitionName, splitValue string) error {
	sql := fmt.Sprintf("ALTER TABLE %s SPLIT PARTITION %s AT ('%s') INTO (PARTITION %s_new, PARTITION %s_old)",
		tableName, partitionName, splitValue, partitionName, partitionName)

	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to split partition: %w", err)
	}
	return nil
}

// ExchangePartition exchanges a partition with a table
func (m *GaussDBPartitionManager) ExchangePartition(ctx context.Context, db *sql.DB, tableName, partitionName, targetTable string) error {
	sql := fmt.Sprintf("ALTER TABLE %s EXCHANGE PARTITION %s WITH TABLE %s",
		tableName, partitionName, targetTable)

	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to exchange partition: %w", err)
	}
	return nil
}

// GetPartitionStatistics retrieves statistics for a partition
func (m *GaussDBPartitionManager) GetPartitionStatistics(ctx context.Context, db *sql.DB, tableName, partitionName string) (*GaussDBPartitionStats, error) {
	sql := `
		SELECT NUM_ROWS, BLOCKS, AVG_ROW_LEN, LAST_ANALYZED
		FROM USER_TAB_PARTITIONS
		WHERE TABLE_NAME = ? AND PARTITION_NAME = ?
	`

	stats := &GaussDBPartitionStats{
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

// GaussDBPartitionStats represents partition statistics
type GaussDBPartitionStats struct {
	TableName     string
	PartitionName string
	NumRows       int64
	Blocks        int64
	AvgRowLen     int
	LastAnalyzed  string
}
