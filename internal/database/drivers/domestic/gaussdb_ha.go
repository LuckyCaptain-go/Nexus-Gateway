package domestic

import (
	"context"
	"database/sql"
	"fmt"
)

// GaussDBHAManager handles GaussDB high availability features
type GaussDBHAManager struct {
	driver *GaussDBDriver
}

// NewGaussDBHAManager creates a new HA manager
func NewGaussDBHAManager(driver *GaussDBDriver) *GaussDBHAManager {
	return &GaussDBHAManager{
		driver: driver,
	}
}

// GetClusterState retrieves the cluster state
func (m *GaussDBHAManager) GetClusterState(ctx context.Context, db *sql.DB) (*GaussDBClusterState, error) {
	sql := `
		SELECT NODE_NAME, NODE_TYPE, NODE_STATE,
		       CURRENT_SCN, REPLAY_SCN
		FROM PG_STAT_REPLICATION
	`

	rows, err := db.QueryContext(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster state: %w", err)
	}
	defer rows.Close()

	state := &GaussDBClusterState{
		Nodes: []GaussDBNodeState{},
	}

	for rows.Next() {
		var node GaussDBNodeState
		err := rows.Scan(
			&node.NodeName,
			&node.NodeType,
			&node.NodeState,
			&node.CurrentSCN,
			&node.ReplaySCN,
		)
		if err != nil {
			return nil, err
		}
		state.Nodes = append(state.Nodes, node)
	}

	return state, nil
}

// GaussDBClusterState represents cluster state
type GaussDBClusterState struct {
	Nodes []GaussDBNodeState
}

// GaussDBNodeState represents node state
type GaussDBNodeState struct {
	NodeName    string
	NodeType    string // primary, standby
	NodeState   string // normal, abnormal, etc.
	CurrentSCN  int64
	ReplaySCN   int64
	Lag         int64
}

// PromoteStandby promotes a standby node to primary
func (m *GaussDBHAManager) PromoteStandby(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, "SELECT pg_promote()")
	if err != nil {
		return fmt.Errorf("failed to promote standby: %w", err)
	}
	return nil
}

// SetupStreamingReplication sets up streaming replication
func (m *GaussDBHAManager) SetupStreamingReplication(ctx context.Context, primaryDB, standbyDB *sql.DB, replicationUser string) error {
	// Create publication on primary
	_, err := primaryDB.ExecContext(ctx, "CREATE PUBLICATION gaussdb_pub FOR ALL TABLES")
	if err != nil {
		return fmt.Errorf("failed to create publication: %w", err)
	}

	// Create subscription on standby
	subSQL := fmt.Sprintf("CREATE SUBSCRIPTION gaussdb_sub CONNECTION 'host=%s user=%s' PUBLICATION gaussdb_pub",
		m.driver.config.Host, replicationUser)
	_, err = standbyDB.ExecContext(ctx, subSQL)
	if err != nil {
		return fmt.Errorf("failed to create subscription: %w", err)
	}

	return nil
}

// FailoverToStandby fails over to a standby node
func (m *GaussDBHAManager) FailoverToStandby(ctx context.Context, standbyHost string) error {
	// In real implementation, you would:
	// 1. Stop primary
	// 2. Promote standby
	// 3. Update application config
	// 4. Redirect traffic

	return fmt.Errorf("failover requires manual intervention")
}

// GetReplicationLag retrieves replication lag
func (m *GaussDBHAManager) GetReplicationLag(ctx context.Context, db *sql.DB) (int64, error) {
	var lag int64
	err := db.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(CURRENT_SCN - REPLAY_SCN), 0) AS lag
		FROM PG_STAT_REPLICATION
	`).Scan(&lag)

	if err != nil {
		return 0, fmt.Errorf("failed to get replication lag: %w", err)
	}

	return lag, nil
}

// IsHealthy checks if the cluster is healthy
func (m *GaussDBHAManager) IsHealthy(ctx context.Context, db *sql.DB) (bool, error) {
	state, err := m.GetClusterState(ctx, db)
	if err != nil {
		return false, err
	}

	// Check if we have at least one primary
	hasPrimary := false
	for _, node := range state.Nodes {
		if node.NodeType == "primary" && node.NodeState == "normal" {
			hasPrimary = true
			break
		}
	}

	return hasPrimary, nil
}

// BuildConnectionFailover builds a connection string with failover support
func (m *GaussDBHAManager) BuildConnectionFailover(config *GaussDBConfig) string {
	dsn := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s",
		config.Host, config.Port, config.Database, config.Username, config.Password)

	// Add standby hosts for failover
	for _, standby := range config.StandbyNodes {
		dsn += " host=" + standby
	}

	dsn += " target_session_attrs=read-write"

	return dsn
}

// EnableSynchronousReplication enables synchronous replication
func (m *GaussDBHAManager) EnableSynchronousReplication(ctx context.Context, db *sql.DB, mode string) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf("SET synchronous_commit = '%s'", mode))
	if err != nil {
		return fmt.Errorf("failed to set synchronous commit: %w", err)
	}
	return nil
}
