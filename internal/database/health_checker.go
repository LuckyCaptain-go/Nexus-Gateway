package database

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/RhinoLink-Tech/data-fabric/internal/database/drivers"
	"github.com/RhinoLink-Tech/data-fabric/internal/model"
)

var (
	sensitiveConnectionParameter = regexp.MustCompile(`(?i)((?:password|passwd|token|secret|credential|access[_-]?key|api[_-]?key)=)[^&\s,;]+`)
	connectionURLPassword        = regexp.MustCompile(`(?i)(https?://[^/\s@]+:)[^@/\s]*@`)
	connectionAuthorization      = regexp.MustCompile(`(?i)(authorization:\s*(?:basic|bearer)\s+)[A-Za-z0-9._~+/-]+=*`)
	sensitiveConnectionKey       = regexp.MustCompile(`(?i)(password|passwd|token|secret|credential|access[_-]?key|api[_-]?key|authorization)`)
)

// redactConnectionError preserves useful connection context while removing configured secrets.
func redactConnectionError(err error, config *model.DataSourceConfig) string {
	if err == nil {
		return ""
	}

	message := err.Error()
	secrets := make([]string, 0)
	if config != nil {
		if config.Password != "" {
			secrets = append(secrets, config.Password)
			if config.Username != "" {
				secrets = append(secrets, base64.StdEncoding.EncodeToString([]byte(config.Username+":"+config.Password)))
			}
		}
		for key, value := range config.AdditionalProps {
			sensitive := model.IsSensitiveDataSourceProperty(key) || sensitiveConnectionKey.MatchString(key)
			collectConnectionSecrets(value, sensitive, &secrets)
		}
	}

	sort.Slice(secrets, func(i, j int) bool { return len(secrets[i]) > len(secrets[j]) })
	for _, secret := range secrets {
		for _, encoded := range []string{secret, url.QueryEscape(secret), url.PathEscape(secret)} {
			if encoded != "" {
				message = strings.ReplaceAll(message, encoded, "[REDACTED]")
			}
		}
	}

	message = connectionURLPassword.ReplaceAllString(message, "$1[REDACTED]@")
	message = sensitiveConnectionParameter.ReplaceAllString(message, "$1[REDACTED]")
	message = connectionAuthorization.ReplaceAllString(message, "$1[REDACTED]")
	return message
}

func collectConnectionSecrets(value interface{}, force bool, secrets *[]string) {
	switch typed := value.(type) {
	case map[string]interface{}:
		for key, child := range typed {
			sensitive := force || model.IsSensitiveDataSourceProperty(key) || sensitiveConnectionKey.MatchString(key)
			collectConnectionSecrets(child, sensitive, secrets)
		}
	case []interface{}:
		for _, child := range typed {
			collectConnectionSecrets(child, force, secrets)
		}
	case string:
		if force && typed != "" {
			*secrets = append(*secrets, typed)
		}
	default:
		if force && value != nil {
			secret := fmt.Sprint(value)
			if secret != "" {
				*secrets = append(*secrets, secret)
			}
		}
	}
}

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
	TotalConnections     int                          `json:"totalConnections"`
	HealthyConnections   int                          `json:"healthyConnections"`
	UnhealthyConnections int                          `json:"unhealthyConnections"`
	Results              []HealthCheckResult          `json:"results"`
	SummaryByType        map[string]TypeHealthSummary `json:"summaryByType"`
	CheckedAt            time.Time                    `json:"checkedAt"`
}

// TypeHealthSummary represents health summary by database type
type TypeHealthSummary struct {
	Total     int `json:"total"`
	Healthy   int `json:"healthy"`
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
		result.Message = fmt.Sprintf("Driver not available: %s", redactConnectionError(err, &dataSource.Config))
		result.Latency = time.Since(startTime)
		return result, nil
	}

	// Get connection from pool
	db, err := hc.connPool.GetConnection(ctx, dataSource)
	if err != nil {
		result.Status = "unhealthy"
		result.Message = fmt.Sprintf("Failed to get connection: %s", redactConnectionError(err, &dataSource.Config))
		result.Latency = time.Since(startTime)
		return result, nil
	}

	// Test connection
	err = driver.TestConnection(db)
	result.Latency = time.Since(startTime)

	if err != nil {
		result.Status = "unhealthy"
		result.Message = fmt.Sprintf("Connection test failed: %s", redactConnectionError(err, &dataSource.Config))
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
		TotalConnections:     len(stats),
		HealthyConnections:   0,
		UnhealthyConnections: 0,
		Results:              make([]HealthCheckResult, 0),
		SummaryByType:        make(map[string]TypeHealthSummary),
		CheckedAt:            time.Now(),
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
		result.Message = fmt.Sprintf("Driver not available: %s", redactConnectionError(err, config))
		result.Latency = time.Since(startTime)
		return result, nil
	}

	// Build DSN
	dsn := driver.BuildDSN(config)

	// Validate DSN
	if err := driver.ValidateDSN(dsn); err != nil {
		result.Status = "error"
		result.Message = fmt.Sprintf("Invalid connection string: %s", redactConnectionError(err, config))
		result.Latency = time.Since(startTime)
		return result, nil
	}

	// Open connection
	db, err := driver.Open(dsn)
	if err != nil {
		result.Status = "unhealthy"
		result.Message = fmt.Sprintf("Failed to open connection: %s", redactConnectionError(err, config))
		result.Latency = time.Since(startTime)
		return result, nil
	}
	defer db.Close()

	// Test connection
	err = driver.TestConnection(db)
	result.Latency = time.Since(startTime)

	if err != nil {
		result.Status = "unhealthy"
		result.Message = fmt.Sprintf("Connection test failed: %s", redactConnectionError(err, config))
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
						Results: []HealthCheckResult{
							{
								Status:    "error",
								Message:   fmt.Sprintf("Health check failed: %v", err),
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

	// Get category to determine validation rules
	category := driver.GetCategory()

	// Validate based on category
	switch category {
	case drivers.CategoryObjectStorage:
		// Object storage (S3, etc.) requires bucket and region
		return hc.validateObjectStorageConfig(config, dbType)
	case drivers.CategoryFileSystem:
		// File systems (HDFS) require namenode and path
		return hc.validateFileSystemConfig(config, dbType)
	default:
		// Standard databases require host, port, database, username
		return hc.validateDatabaseConfig(config, driver)
	}
}

// validateDatabaseConfig validates standard database configurations
func (hc *HealthChecker) validateDatabaseConfig(config *model.DataSourceConfig, driver drivers.Driver) error {
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

// validateObjectStorageConfig validates object storage configurations
func (hc *HealthChecker) validateObjectStorageConfig(config *model.DataSourceConfig, dbType model.DatabaseType) error {
	// Object storage requires bucket name and region
	if config.Database == "" { // Use Database field for bucket name
		return fmt.Errorf("bucket name is required")
	}

	// Region is required for most object storage providers
	// (stored in Host field for non-SQL sources)
	if config.Host == "" {
		return fmt.Errorf("region is required")
	}

	return nil
}

// validateFileSystemConfig validates file system configurations
func (hc *HealthChecker) validateFileSystemConfig(config *model.DataSourceConfig, dbType model.DatabaseType) error {
	// HDFS/Ozone require namenode/ozonename address
	if config.Host == "" {
		return fmt.Errorf("namenode address is required")
	}

	// Port is required
	if config.Port <= 0 {
		config.Port = 8020 // Default HDFS port
	}

	// Path is optional (uses root if not specified)
	if config.Database == "" {
		config.Database = "/" // Default to root
	}

	return nil
}

// GetDriverInfo returns information about available database drivers
func (hc *HealthChecker) GetDriverInfo() map[string]DriverInfo {
	supportedTypes := hc.registry.GetSupportedTypes()
	info := make(map[string]DriverInfo)

	for _, dbType := range supportedTypes {
		driver, _ := hc.registry.GetDriver(dbType)
		info[string(dbType)] = DriverInfo{
			Type:        string(dbType),
			DriverName:  driver.GetDriverName(),
			DefaultPort: driver.GetDefaultPort(),
			Supported:   true,
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
