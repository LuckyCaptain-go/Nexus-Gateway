package database

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"nexus-gateway/internal/model"
)

// ConnectionPool manages database connections for different data sources
type ConnectionPool struct {
	pools    map[string]*sql.DB
	mutex    sync.RWMutex
	health   map[string]bool
	healthMu sync.RWMutex
}

// NewConnectionPool creates a new ConnectionPool instance
func NewConnectionPool() *ConnectionPool {
	return &ConnectionPool{
		pools:  make(map[string]*sql.DB),
		health: make(map[string]bool),
	}
}

// GetConnection gets or creates a database connection for the specified data source
func (cp *ConnectionPool) GetConnection(ctx context.Context, dataSource *model.DataSource) (*sql.DB, error) {
	cp.mutex.RLock()
	if db, exists := cp.pools[dataSource.ID]; exists {
		cp.mutex.RUnlock()

		// Check if connection is still alive
		if err := db.PingContext(ctx); err == nil {
			return db, nil
		}

		// Connection is dead, remove and recreate
		cp.mutex.RUnlock()
		cp.removeConnection(dataSource.ID)
	} else {
		cp.mutex.RUnlock()
	}

	// Create new connection
	return cp.createConnection(ctx, dataSource)
}

// createConnection creates a new database connection pool
func (cp *ConnectionPool) createConnection(ctx context.Context, dataSource *model.DataSource) (*sql.DB, error) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	// Double-check after acquiring write lock
	if db, exists := cp.pools[dataSource.ID]; exists {
		if err := db.PingContext(ctx); err == nil {
			return db, nil
		}
	}

	// Get connection string
	dsn := dataSource.Config.GetConnectionURL(dataSource.Type)
	if dsn == "" {
		return nil, fmt.Errorf("unsupported database type: %s", dataSource.Type)
	}

	// Open database connection
	db, err := cp.openDatabase(dataSource.Type, dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	// Configure connection pool
	if err := cp.configureConnectionPool(db, &dataSource.Config); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to configure connection pool: %w", err)
	}

	// Test the connection
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Store in pool
	cp.pools[dataSource.ID] = db
	cp.setHealth(dataSource.ID, true)

	return db, nil
}

// openDatabase opens a database connection based on database type
func (cp *ConnectionPool) openDatabase(dbType model.DatabaseType, dsn string) (*sql.DB, error) {
	driver, err := GetDriverRegistry().GetDriver(dbType)
	if err != nil {
		return nil, err
	}

	return driver.Open(dsn)
}

// configureConnectionPool configures the connection pool settings
func (cp *ConnectionPool) configureConnectionPool(db *sql.DB, config *model.DataSourceConfig) error {
	// Set connection pool size
	maxOpenConns := config.MaxPoolSize
	if maxOpenConns <= 0 {
		maxOpenConns = 10 // Default
	}
	db.SetMaxOpenConns(maxOpenConns)

	// Set maximum idle connections
	maxIdleConns := maxOpenConns / 2
	if maxIdleConns < 2 {
		maxIdleConns = 2
	}
	db.SetMaxIdleConns(maxIdleConns)

	// Set connection lifetime
	maxLifetime := time.Duration(config.MaxLifetime) * time.Second
	if maxLifetime <= 0 {
		maxLifetime = 30 * time.Minute // Default
	}
	db.SetConnMaxLifetime(maxLifetime)

	// Set connection timeout
	timeout := time.Duration(config.Timeout) * time.Second
	if timeout <= 0 {
		timeout = 30 * time.Second // Default
	}

	// Note: SetConnMaxIdleTime is not available in all Go versions
	// We'll handle it gracefully
	db.SetConnMaxIdleTime(timeout)

	return nil
}

// removeConnection removes a connection from the pool
func (cp *ConnectionPool) removeConnection(dataSourceID string) {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	if db, exists := cp.pools[dataSourceID]; exists {
		db.Close()
		delete(cp.pools, dataSourceID)
		cp.setHealth(dataSourceID, false)
	}
}

// CloseConnection closes a specific connection and removes it from the pool
func (cp *ConnectionPool) CloseConnection(dataSourceID string) error {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	if db, exists := cp.pools[dataSourceID]; exists {
		err := db.Close()
		delete(cp.pools, dataSourceID)
		cp.setHealth(dataSourceID, false)
		return err
	}

	return fmt.Errorf("connection not found for data source: %s", dataSourceID)
}

// CloseAll closes all connections in the pool
func (cp *ConnectionPool) CloseAll() error {
	cp.mutex.Lock()
	defer cp.mutex.Unlock()

	var lastErr error
	for id, db := range cp.pools {
		if err := db.Close(); err != nil {
			lastErr = err
		}
		delete(cp.pools, id)
		cp.setHealth(id, false)
	}

	return lastErr
}

// GetStats returns statistics for all connections in the pool
func (cp *ConnectionPool) GetStats() map[string]ConnectionStats {
	cp.mutex.RLock()
	defer cp.mutex.RUnlock()

	stats := make(map[string]ConnectionStats)
	for id, db := range cp.pools {
		dbStats := db.Stats()
		stats[id] = ConnectionStats{
			OpenConnections: dbStats.OpenConnections,
			InUse:          dbStats.InUse,
			Idle:           dbStats.Idle,
			WaitCount:      dbStats.WaitCount,
			WaitDuration:   dbStats.WaitDuration,
			MaxIdleClosed:  dbStats.MaxIdleClosed,
			MaxLifetimeClosed: dbStats.MaxLifetimeClosed,
			Healthy:        cp.getHealth(id),
		}
	}

	return stats
}

// ConnectionStats contains connection pool statistics
type ConnectionStats struct {
	OpenConnections   int           `json:"openConnections"`
	InUse            int           `json:"inUse"`
	Idle             int           `json:"idle"`
	WaitCount        int64         `json:"waitCount"`
	WaitDuration     time.Duration `json:"waitDuration"`
	MaxIdleClosed    int64         `json:"maxIdleClosed"`
	MaxLifetimeClosed int64        `json:"maxLifetimeClosed"`
	Healthy          bool          `json:"healthy"`
}

// HealthCheck performs health checks on all connections
func (cp *ConnectionPool) HealthCheck(ctx context.Context) map[string]bool {
	cp.mutex.RLock()
	defer cp.mutex.RUnlock()

	results := make(map[string]bool)

	for id, db := range cp.pools {
		if err := db.PingContext(ctx); err != nil {
			cp.setHealth(id, false)
			results[id] = false
		} else {
			cp.setHealth(id, true)
			results[id] = true
		}
	}

	return results
}

// IsHealthy checks if a specific data source connection is healthy
func (cp *ConnectionPool) IsHealthy(dataSourceID string) bool {
	cp.healthMu.RLock()
	defer cp.healthMu.RUnlock()
	return cp.health[dataSourceID]
}

// setHealth sets the health status for a connection
func (cp *ConnectionPool) setHealth(dataSourceID string, healthy bool) {
	cp.healthMu.Lock()
	defer cp.healthMu.Unlock()
	cp.health[dataSourceID] = healthy
}

// getHealth gets the health status for a connection
func (cp *ConnectionPool) getHealth(dataSourceID string) bool {
	cp.healthMu.RLock()
	defer cp.healthMu.RUnlock()
	return cp.health[dataSourceID]
}