package domestic

import (
	"context"
	"database/sql"
	"fmt"
)

// TDSQLShardingManager handles TDSQL sharding features
type TDSQLShardingManager struct {
	driver *TDSQLDriver
}

// NewTDSQLShardingManager creates a new sharding manager
func NewTDSQLShardingManager(driver *TDSQLDriver) *TDSQLShardingManager {
	return &TDSQLShardingManager{
		driver: driver,
	}
}

// GetShardingInfo retrieves sharding information for a table
func (m *TDSQLShardingManager) GetShardingInfo(ctx context.Context, db *sql.DB, tableName string) (*TDSQLShardingInfo, error) {
	sql := `
		SELECT TABLE_NAME, SHARD_KEY, SHARD_COUNT,
		       SHARD_TYPE, REPLICA_COUNT
		FROM information_schema.TDSQL_SHARDING
		WHERE TABLE_NAME = ?
	`

	info := &TDSQLShardingInfo{
		TableName: tableName,
	}

	err := db.QueryRowContext(ctx, sql, tableName).Scan(
		&info.TableName,
		&info.ShardKey,
		&info.ShardCount,
		&info.ShardType,
		&info.ReplicaCount,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get sharding info: %w", err)
	}

	return info, nil
}

// TDSQLShardingInfo represents sharding information
type TDSQLShardingInfo struct {
	TableName    string
	ShardKey     string
	ShardCount   int
	ShardType    string // HASH, RANGE, LIST
	ReplicaCount int
}

// ExecuteRoutedQuery executes a query with explicit shard routing
func (m *TDSQLShardingManager) ExecuteRoutedQuery(ctx context.Context, db *sql.DB, sql string, shardValue interface{}) (*TDSQLQueryResult, error) {
	// TDSQL will automatically route based on shard key in WHERE clause
	rows, err := db.QueryContext(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to execute routed query: %w", err)
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

	return &TDSQLQueryResult{
		Rows:     results,
		Columns:  columns,
		Count:    len(results),
		ShardKey: fmt.Sprintf("%v", shardValue),
		IsRouted: true,
	}, nil
}

// GetShardNodes retrieves information about shard nodes
func (m *TDSQLShardingManager) GetShardNodes(ctx context.Context, db *sql.DB) ([]TDSQLShardNode, error) {
	sql := `
		SELECT NODE_ID, NODE_HOST, NODE_PORT,
		       NODE_STATUS, SHARD_COUNT
		FROM information_schema.TDSQL_NODES
	`

	rows, err := db.QueryContext(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to get shard nodes: %w", err)
	}
	defer rows.Close()

	var nodes []TDSQLShardNode

	for rows.Next() {
		var node TDSQLShardNode
		err := rows.Scan(
			&node.NodeID,
			&node.NodeHost,
			&node.NodePort,
			&node.NodeStatus,
			&node.ShardCount,
		)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}

	return nodes, nil
}

// TDSQLShardNode represents a shard node
type TDSQLShardNode struct {
	NodeID     string
	NodeHost   string
	NodePort   int
	NodeStatus string
	ShardCount int
}

// BroadcastQuery broadcasts a query to all shards
func (m *TDSQLShardingManager) BroadcastQuery(ctx context.Context, db *sql.DB, sql string) (*TDSQLQueryResult, error) {
	// Add hint to broadcast query
	broadcastSQL := "/*+ BROADCAST */ " + sql

	rows, err := db.QueryContext(ctx, broadcastSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to execute broadcast query: %w", err)
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

	return &TDSQLQueryResult{
		Rows:     results,
		Columns:  columns,
		Count:    len(results),
		IsRouted: false,
	}, nil
}

// SetReadWriteSplitting configures read-write splitting
func (m *TDSQLShardingManager) SetReadWriteSplitting(ctx context.Context, db *sql.DB, enabled bool) error {
	mode := "OFF"
	if enabled {
		mode = "ON"
	}

	_, err := db.ExecContext(ctx, fmt.Sprintf("SET read_write_split = %s", mode))
	if err != nil {
		return fmt.Errorf("failed to set read-write splitting: %w", err)
	}
	return nil
}

// GetConsistencyLevel gets the current consistency level
func (m *TDSQLShardingManager) GetConsistencyLevel(ctx context.Context, db *sql.DB) (string, error) {
	var level string
	err := db.QueryRowContext(ctx, "SELECT @@read_consistency").Scan(&level)
	if err != nil {
		return "", fmt.Errorf("failed to get consistency level: %w", err)
	}
	return level, nil
}

// SetConsistencyLevel sets the consistency level
func (m *TDSQLShardingManager) SetConsistencyLevel(ctx context.Context, db *sql.DB, level string) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf("SET read_consistency = '%s'", level))
	if err != nil {
		return fmt.Errorf("failed to set consistency level: %w", err)
	}
	return nil
}
