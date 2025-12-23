package database

import (
	"context"
	"fmt"
	"time"

	"nexus-gateway/internal/model"
)

// HealthChecker performs health checks on database connections
type HealthChecker struct {
	connPool *ConnectionPool
	registry *DriverRegistry
}

// NewHealthChecker creates a new HealthChecker instance
func NewHealthChecker(connPool *ConnectionPool) *HealthChecker {
	return &HealthChecker{
		connPool: connPool,
		registry: GetDriverRegistry(),
	}
}

// HealthCheckResult represents the result of a health check
type HealthCheckResult struct {
	DataSourceID string        `json:"dataSourceId"`
	DatabaseType string        `json:"databaseType"`
	Status       string        `json:"status"`
	Message      string        `json:"message,omitempty"`
	Latency      time.Duration `json:"latency"`
	CheckedAt    time.Time     `json:"checkedAt"`
}

// DatabaseHealthSummary represents a summary of database health
type DatabaseHealthSummary struct {
	TotalConnections int                `json:"totalConnections"`
	HealthyConnections int             `json:"healthyConnections"`
	UnhealthyConnections int           `json:"unhealthyConnections"`
	Results           []HealthCheckResult `json:"results"`
	SummaryByType     map[string]TypeHealthSummary `json:"summaryByType"`
	CheckedAt         time.Time                `json:"checkedAt"`
}

// TypeHealthSummary represents health summary by database type
type TypeHealthSummary struct {
	Total   int `json:"total"`
	Healthy int `json:"healthy"`
	Unhealthy int `json:"unhealthy"`
}

// CheckDataSourceHealth checks the health of a specific data source
func (hc *HealthChecker) CheckDataSourceHealth(ctx context.Context, dataSource *model.DataSource) (*HealthCheckResult, error) {
	startTime := time.Now()

	result := &HealthCheckResult{
		DataSourceID: dataSource.ID,
		DatabaseType: string(dataSource.Type),
		CheckedAt:    time.Now(),
	}

	// Get driver for the database type
	driver, err := hc.registry.GetDriver(dataSource.Type)
	if err != nil {
		result.Status = "error"
		result.Message = fmt.Sprintf("Driver not available: %v", err)
		result.Latency = time.Since(startTime)
		return result, nil
	}

	// Get connection from pool
	db, err := hc.connPool.GetConnection(ctx, dataSource)
	if err != nil {
		result.Status = "unhealthy"
		result.Message = fmt.Sprintf("Failed to get connection: %v", err)
		result.Latency = time.Since(startTime)
		return result, nil
	}

	// Test connection
	err = driver.TestConnection(db)
	result.Latency = time.Since(startTime)

	if err != nil {
		result.Status = "unhealthy"
		result.Message = fmt.Sprintf("Connection test failed: %v", err)
	} else {
		result.Status = "healthy"
		result.Message = "Connection successful"
	}

	return result, nil
}

// CheckAllConnectionsHealth checks health of all active connections in the pool
func (hc *HealthChecker) CheckAllConnectionsHealth(ctx context.Context) (*DatabaseHealthSummary, error) {
	stats := hc.connPool.GetStats()

	summary := &DatabaseHealthSummary{
		TotalConnections:    len(stats),
		HealthyConnections:  0,
		UnhealthyConnections: 0,
		Results:             make([]HealthCheckResult, 0),
		SummaryByType:       make(map[string]TypeHealthSummary),
		CheckedAt:           time.Now(),
	}

	// Check each connection
	for dataSourceID, connStats := range stats {
		result := HealthCheckResult{
			DataSourceID: dataSourceID,
			Status:       "unknown",
			Latency:      0,
			CheckedAt:    time.Now(),
		}

		if connStats.Healthy {
			result.Status = "healthy"
			result.Message = "Connection is healthy"
			summary.HealthyConnections++
		} else {
			result.Status = "unhealthy"
			result.Message = "Connection is unhealthy"
			summary.UnhealthyConnections++
		}

		summary.Results = append(summary.Results, result)
	}

	return summary, nil
}

// CheckDataSourceConnectivity tests connectivity to a data source without using pool
func (hc *HealthChecker) CheckDataSourceConnectivity(ctx context.Context, config *model.DataSourceConfig, dbType model.DatabaseType) (*HealthCheckResult, error) {
	startTime := time.Now()

	result := &HealthCheckResult{
		DatabaseType: string(dbType),
		CheckedAt:    time.Now(),
	}

	// Get driver for the database type
	driver, err := hc.registry.GetDriver(dbType)
	if err != nil {
		result.Status = "error"
		result.Message = fmt.Sprintf("Driver not available: %v", err)
		result.Latency = time.Since(startTime)
		return result, nil
	}

	// Build DSN
	dsn := driver.BuildDSN(config)

	// Validate DSN
	if err := driver.ValidateDSN(dsn); err != nil {
		result.Status = "error"
		result.Message = fmt.Sprintf("Invalid connection string: %v", err)
		result.Latency = time.Since(startTime)
		return result, nil
	}

	// Open connection
	db, err := driver.Open(dsn)
	if err != nil {
		result.Status = "unhealthy"
		result.Message = fmt.Sprintf("Failed to open connection: %v", err)
		result.Latency = time.Since(startTime)
		return result, nil
	}
	defer db.Close()

	// Test connection
	err = driver.TestConnection(db)
	result.Latency = time.Since(startTime)

	if err != nil {
		result.Status = "unhealthy"
		result.Message = fmt.Sprintf("Connection test failed: %v", err)
	} else {
		result.Status = "healthy"
		result.Message = "Connection successful"
	}

	return result, nil
}

// PeriodicHealthCheck performs periodic health checks
func (hc *HealthChecker) PeriodicHealthCheck(ctx context.Context, interval time.Duration) <-chan *DatabaseHealthSummary {
	results := make(chan *DatabaseHealthSummary)

	go func() {
		defer close(results)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				summary, err := hc.CheckAllConnectionsHealth(ctx)
				if err != nil {
					// Create error summary
					summary = &DatabaseHealthSummary{
						CheckedAt: time.Now(),
						Results:   []HealthCheckResult{
							{
								Status:  "error",
								Message: fmt.Sprintf("Health check failed: %v", err),
								CheckedAt: time.Now(),
							},
						},
					}
				}
				select {
				case results <- summary:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return results
}

// ValidateDataSourceConfiguration validates a data source configuration
func (hc *HealthChecker) ValidateDataSourceConfiguration(config *model.DataSourceConfig, dbType model.DatabaseType) error {
	// Get driver for the database type
	driver, err := hc.registry.GetDriver(dbType)
	if err != nil {
		return fmt.Errorf("driver not available for database type %s: %w", dbType, err)
	}

	// Check required fields
	if config.Host == "" {
		return fmt.Errorf("host is required")
	}
	if config.Port <= 0 {
		config.Port = driver.GetDefaultPort()
	}
	if config.Database == "" {
		return fmt.Errorf("database name is required")
	}
	if config.Username == "" {
		return fmt.Errorf("username is required")
	}

	// Build and validate DSN
	dsn := driver.BuildDSN(config)
	return driver.ValidateDSN(dsn)
}

// GetDriverInfo returns information about available database drivers
func (hc *HealthChecker) GetDriverInfo() map[string]DriverInfo {
	supportedTypes := hc.registry.GetSupportedTypes()
	info := make(map[string]DriverInfo)

	for _, dbType := range supportedTypes {
		driver, _ := hc.registry.GetDriver(dbType)
		info[string(dbType)] = DriverInfo{
			Type:         string(dbType),
			DriverName:   driver.GetDriverName(),
			DefaultPort:  driver.GetDefaultPort(),
			Supported:    true,
		}
	}

	return info
}

// DriverInfo contains information about a database driver
type DriverInfo struct {
	Type        string `json:"type"`
	DriverName  string `json:"driverName"`
	DefaultPort int    `json:"defaultPort"`
	Supported   bool   `json:"supported"`
}