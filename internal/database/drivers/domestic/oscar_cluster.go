package domestic

import (
	"context"
	"database/sql"
	"fmt"
)

// OscarClusterManager handles Oscar cluster features
type OscarClusterManager struct {
	driver *OscarDriver
}

// NewOscarClusterManager creates a new cluster manager
func NewOscarClusterManager(driver *OscarDriver) *OscarClusterManager {
	return &OscarClusterManager{
		driver: driver,
	}
}

// GetClusterStatus retrieves cluster status
func (m *OscarClusterManager) GetClusterStatus(ctx context.Context, db *sql.DB) (*OscarClusterStatus, error) {
	if !m.driver.config.ClusterMode {
		return nil, fmt.Errorf("cluster mode is not enabled")
	}

	sql := `
		SELECT NODE_ID, NODE_NAME, NODE_ROLE,
		       NODE_STATUS, CURRENT_LOAD
		FROM SYSTEM_CLUSTER_NODES
		ORDER BY NODE_ID
	`

	rows, err := db.QueryContext(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster status: %w", err)
	}
	defer rows.Close()

	status := &OscarClusterStatus{
		Nodes: []OscarNode{},
	}

	for rows.Next() {
		var node OscarNode
		err := rows.Scan(
			&node.NodeID,
			&node.NodeName,
			&node.NodeRole,
			&node.Status,
			&node.CurrentLoad,
		)
		if err != nil {
			return nil, err
		}
		status.Nodes = append(status.Nodes, node)
	}

	return status, nil
}

// OscarClusterStatus represents cluster status
type OscarClusterStatus struct {
	Nodes       []OscarNode
	TotalNodes  int
	ClusterName string
}

// OscarNode represents a cluster node
type OscarNode struct {
	NodeID      string
	NodeName    string
	NodeRole    string // coordinator, worker
	Status      string
	CurrentLoad float64
}

// AddNode adds a new node to the cluster
func (m *OscarClusterManager) AddNode(ctx context.Context, db *sql.DB, nodeName, nodeHost string, nodePort int) error {
	sql := fmt.Sprintf("EXECUTE PROCEDURE ADD_CLUSTER_NODE('%s', '%s', %d)", nodeName, nodeHost, nodePort)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to add node: %w", err)
	}
	return nil
}

// RemoveNode removes a node from the cluster
func (m *OscarClusterManager) RemoveNode(ctx context.Context, db *sql.DB, nodeName string) error {
	sql := fmt.Sprintf("EXECUTE PROCEDURE REMOVE_CLUSTER_NODE('%s')", nodeName)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to remove node: %w", err)
	}
	return nil
}

// RebalanceCluster rebalances data across cluster nodes
func (m *OscarClusterManager) RebalanceCluster(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, "EXECUTE PROCEDURE REBALANCE_CLUSTER()")
	if err != nil {
		return fmt.Errorf("failed to rebalance cluster: %w", err)
	}
	return nil
}

// GetNodeLoad retrieves load information for a specific node
func (m *OscarClusterManager) GetNodeLoad(ctx context.Context, db *sql.DB, nodeName string) (*OscarNodeLoad, error) {
	sql := `
		SELECT NODE_NAME, CPU_USAGE, MEMORY_USAGE,
		       DISK_USAGE, CONNECTION_COUNT
		FROM SYSTEM_CLUSTER_NODE_LOAD
		WHERE NODE_NAME = ?
	`

	load := &OscarNodeLoad{
		NodeName: nodeName,
	}

	err := db.QueryRowContext(ctx, sql, nodeName).Scan(
		&load.NodeName,
		&load.CPUUsage,
		&load.MemoryUsage,
		&load.DiskUsage,
		&load.ConnectionCount,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get node load: %w", err)
	}

	return load, nil
}

// OscarNodeLoad represents node load information
type OscarNodeLoad struct {
	NodeName        string
	CPUUsage        float64
	MemoryUsage     float64
	DiskUsage       float64
	ConnectionCount int
}

// FailoverNode initiates failover to a standby node
func (m *OscarClusterManager) FailoverNode(ctx context.Context, db *sql.DB, fromNode, toNode string) error {
	sql := fmt.Sprintf("EXECUTE PROCEDURE FAILOVER_NODE('%s', '%s')", fromNode, toNode)
	_, err := db.ExecContext(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to failover node: %w", err)
	}
	return nil
}

// EnableClusterMode enables cluster mode
func (m *OscarClusterManager) EnableClusterMode(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, "SET CLUSTER_MODE = ON")
	if err != nil {
		return fmt.Errorf("failed to enable cluster mode: %w", err)
	}
	return nil
}
