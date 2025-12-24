package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"nexus-gateway/internal/model"
)

// MetricsCollector collects and aggregates query execution metrics
type MetricsCollector struct {
	metrics      map[string]*DataSourceMetrics
	metricsMutex sync.RWMutex

	globalMetrics *GlobalMetrics
	globalMutex   sync.RWMutex

	retentionDuration time.Duration
	cleanupInterval  time.Duration
}

// DataSourceMetrics holds metrics for a specific data source
type DataSourceMetrics struct {
	DataSourceID          string
	DatabaseType          model.DatabaseType
	TotalQueries          int64
	SuccessfulQueries     int64
	FailedQueries         int64
	TotalExecutionTimeNs  int64
	MinExecutionTimeNs    int64
	MaxExecutionTimeNs    int64
	AvgExecutionTimeNs    int64
	TotalRowsRead         int64
	TotalBytesRead        int64
	LastQueryTime         time.Time
	LastError             string
	LastErrorTime         time.Time
	CurrentConnections    int
	MaxConnections        int
	QueriesByHour         map[int64]int64 // Hour -> count
	StatusChanges         []StatusChange
}

// StatusChange represents a status change event
type StatusChange struct {
	FromStatus string
	ToStatus   string
	ChangedAt  time.Time
	Reason     string
}

// GlobalMetrics holds gateway-wide metrics
type GlobalMetrics struct {
	TotalQueries          int64
	SuccessfulQueries     int64
	FailedQueries         int64
	TotalExecutionTimeNs  int64
	QueriesByDataSource   map[string]int64
	QueriesByType         map[model.DatabaseType]int64
	QueriesByHour         map[int64]int64
	ActiveConnections     int
	StartTime             time.Time
}

// QueryMetrics represents metrics for a single query execution
type QueryMetrics struct {
	QueryID              string
	DataSourceID         string
	DatabaseType         model.DatabaseType
	SQL                  string
	Success              bool
	ExecutionTimeNs      int64
	RowsRead             int64
	BytesRead            int64
	Error                string
	Timestamp            time.Time
}

// NewMetricsCollector creates a new metrics collector
func NewMetricsCollector(retention time.Duration) *MetricsCollector {
	return &MetricsCollector{
		metrics:          make(map[string]*DataSourceMetrics),
		retentionDuration: retention,
		cleanupInterval:   1 * time.Hour,
		globalMetrics: &GlobalMetrics{
			QueriesByDataSource: make(map[string]int64),
			QueriesByType:       make(map[model.DatabaseType]int64),
			QueriesByHour:       make(map[int64]int64),
			StartTime:          time.Now(),
		},
	}
}

// RecordQuery records metrics for a query execution
func (mc *MetricsCollector) RecordQuery(metrics *QueryMetrics) {
	if metrics == nil {
		return
	}

	// Update data source metrics
	mc.metricsMutex.Lock()
	dsMetrics, exists := mc.metrics[metrics.DataSourceID]
	if !exists {
		dsMetrics = &DataSourceMetrics{
			DataSourceID:      metrics.DataSourceID,
			DatabaseType:      metrics.DatabaseType,
			MinExecutionTimeNs: metrics.ExecutionTimeNs,
			MaxExecutionTimeNs: metrics.ExecutionTimeNs,
			QueriesByHour:     make(map[int64]int64),
			StatusChanges:     []StatusChange{},
		}
		mc.metrics[metrics.DataSourceID] = dsMetrics
	}

	// Update counters
	dsMetrics.TotalQueries++
	dsMetrics.TotalExecutionTimeNs += metrics.ExecutionTimeNs
	dsMetrics.TotalRowsRead += metrics.RowsRead
	dsMetrics.TotalBytesRead += metrics.BytesRead
	dsMetrics.LastQueryTime = metrics.Timestamp

	if metrics.Success {
		dsMetrics.SuccessfulQueries++
	} else {
		dsMetrics.FailedQueries++
		dsMetrics.LastError = metrics.Error
		dsMetrics.LastErrorTime = metrics.Timestamp
	}

	// Update min/max execution time
	if metrics.ExecutionTimeNs < dsMetrics.MinExecutionTimeNs {
		dsMetrics.MinExecutionTimeNs = metrics.ExecutionTimeNs
	}
	if metrics.ExecutionTimeNs > dsMetrics.MaxExecutionTimeNs {
		dsMetrics.MaxExecutionTimeNs = metrics.ExecutionTimeNs
	}

	// Calculate average
	dsMetrics.AvgExecutionTimeNs = dsMetrics.TotalExecutionTimeNs / dsMetrics.TotalQueries

	// Update hourly stats
	hour := metrics.Timestamp.Truncate(time.Hour).Unix()
	dsMetrics.QueriesByHour[hour]++

	mc.metricsMutex.Unlock()

	// Update global metrics
	mc.globalMutex.Lock()
	mc.globalMetrics.TotalQueries++
	mc.globalMetrics.TotalExecutionTimeNs += metrics.ExecutionTimeNs

	if metrics.Success {
		mc.globalMetrics.SuccessfulQueries++
	} else {
		mc.globalMetrics.FailedQueries++
	}

	mc.globalMetrics.QueriesByDataSource[metrics.DataSourceID]++
	mc.globalMetrics.QueriesByType[metrics.DatabaseType]++
	mc.globalMetrics.QueriesByHour[hour]++

	mc.globalMutex.Unlock()
}

// GetDataSourceMetrics returns metrics for a specific data source
func (mc *MetricsCollector) GetDataSourceMetrics(dataSourceID string) (*DataSourceMetrics, error) {
	mc.metricsMutex.RLock()
	defer mc.metricsMutex.RUnlock()

	metrics, exists := mc.metrics[dataSourceID]
	if !exists {
		return nil, ErrDataSourceMetricsNotFound
	}

	// Return a copy to avoid race conditions
	copy := *metrics
	copy.QueriesByHour = make(map[int64]int64)
	for k, v := range metrics.QueriesByHour {
		copy.QueriesByHour[k] = v
	}

	return &copy, nil
}

// GetAllMetrics returns metrics for all data sources
func (mc *MetricsCollector) GetAllMetrics() map[string]*DataSourceMetrics {
	mc.metricsMutex.RLock()
	defer mc.metricsMutex.RUnlock()

	result := make(map[string]*DataSourceMetrics)
	for id, metrics := range mc.metrics {
		copy := *metrics
		copy.QueriesByHour = make(map[int64]int64)
		for k, v := range metrics.QueriesByHour {
			copy.QueriesByHour[k] = v
		}
		result[id] = &copy
	}

	return result
}

// GetGlobalMetrics returns global gateway metrics
func (mc *MetricsCollector) GetGlobalMetrics() *GlobalMetrics {
	mc.globalMutex.RLock()
	defer mc.globalMutex.RUnlock()

	copy := *mc.globalMetrics
	copy.QueriesByDataSource = make(map[string]int64)
	copy.QueriesByType = make(map[model.DatabaseType]int64)
	copy.QueriesByHour = make(map[int64]int64)

	for k, v := range mc.globalMetrics.QueriesByDataSource {
		copy.QueriesByDataSource[k] = v
	}
	for k, v := range mc.globalMetrics.QueriesByType {
		copy.QueriesByType[k] = v
	}
	for k, v := range mc.globalMetrics.QueriesByHour {
		copy.QueriesByHour[k] = v
	}

	return &copy
}

// UpdateConnectionCount updates the current connection count for a data source
func (mc *MetricsCollector) UpdateConnectionCount(dataSourceID string, current, max int) {
	mc.metricsMutex.Lock()
	defer mc.metricsMutex.Unlock()

	metrics, exists := mc.metrics[dataSourceID]
	if !exists {
		metrics = &DataSourceMetrics{
			DataSourceID:  dataSourceID,
			QueriesByHour: make(map[int64]int64),
			StatusChanges:  []StatusChange{},
		}
		mc.metrics[dataSourceID] = metrics
	}

	metrics.CurrentConnections = current
	metrics.MaxConnections = max
}

// RecordStatusChange records a status change for a data source
func (mc *MetricsCollector) RecordStatusChange(dataSourceID, fromStatus, toStatus, reason string) {
	mc.metricsMutex.Lock()
	defer mc.metricsMutex.Unlock()

	metrics, exists := mc.metrics[dataSourceID]
	if !exists {
		metrics = &DataSourceMetrics{
			DataSourceID:  dataSourceID,
			QueriesByHour: make(map[int64]int64),
			StatusChanges:  []StatusChange{},
		}
		mc.metrics[dataSourceID] = metrics
	}

	change := StatusChange{
		FromStatus: fromStatus,
		ToStatus:   toStatus,
		ChangedAt:  time.Now(),
		Reason:     reason,
	}

	metrics.StatusChanges = append(metrics.StatusChanges, change)

	// Keep only last 100 status changes
	if len(metrics.StatusChanges) > 100 {
		metrics.StatusChanges = metrics.StatusChanges[len(metrics.StatusChanges)-100:]
	}
}

// CleanupOldMetrics removes metrics older than the retention period
func (mc *MetricsCollector) CleanupOldMetrics() {
	mc.metricsMutex.Lock()
	defer mc.metricsMutex.Unlock()

	cutoffTime := time.Now().Add(-mc.retentionDuration).Unix()

	for _, metrics := range mc.metrics {
		// Clean up hourly stats
		for hour := range metrics.QueriesByHour {
			if hour < cutoffTime {
				delete(metrics.QueriesByHour, hour)
			}
		}
	}

	mc.globalMutex.Lock()
	defer mc.globalMutex.Unlock()

	// Clean up global hourly stats
	for hour := range mc.globalMetrics.QueriesByHour {
		if hour < cutoffTime {
			delete(mc.globalMetrics.QueriesByHour, hour)
		}
	}
}

// ResetMetrics resets all metrics for a data source
func (mc *MetricsCollector) ResetMetrics(dataSourceID string) {
	mc.metricsMutex.Lock()
	defer mc.metricsMutex.Unlock()

	if metrics, exists := mc.metrics[dataSourceID]; exists {
		// Preserve some fields
		dsID := metrics.DataSourceID
		dbType := metrics.DatabaseType
		statusChanges := metrics.StatusChanges

		// Reset counters
		*metrics = DataSourceMetrics{
			DataSourceID:  dsID,
			DatabaseType:  dbType,
			QueriesByHour: make(map[int64]int64),
			StatusChanges:  statusChanges,
		}
	}
}

// GetMetricsSummary returns a summary of metrics
func (mc *MetricsCollector) GetMetricsSummary() map[string]interface{} {
	global := mc.GetGlobalMetrics()

	uptime := time.Since(global.StartTime)

	summary := map[string]interface{}{
		"uptime_seconds":            uptime.Seconds(),
		"total_queries":             global.TotalQueries,
		"successful_queries":        global.SuccessfulQueries,
		"failed_queries":            global.FailedQueries,
		"success_rate":              0.0,
		"avg_execution_time_ms":     0.0,
		"queries_per_second":        0.0,
		"active_data_sources":       len(global.QueriesByDataSource),
		"queries_by_data_source":    global.QueriesByDataSource,
		"queries_by_database_type":  global.QueriesByType,
	}

	if global.TotalQueries > 0 {
		summary["success_rate"] = float64(global.SuccessfulQueries) / float64(global.TotalQueries)
		summary["avg_execution_time_ms"] = (float64(global.TotalExecutionTimeNs) / float64(global.TotalQueries)) / 1e6
		summary["queries_per_second"] = float64(global.TotalQueries) / uptime.Seconds()
	}

	return summary
}

// GetTopDataSources returns top data sources by query count
func (mc *MetricsCollector) GetTopDataSources(limit int) []string {
	mc.globalMutex.RLock()
	defer mc.globalMutex.RUnlock()

	type dsCount struct {
		id    string
		count int64
	}

	var counts []dsCount
	for id, count := range mc.globalMetrics.QueriesByDataSource {
		counts = append(counts, dsCount{id, count})
	}

	// Sort by count (descending)
	// Simple bubble sort for small datasets
	for i := 0; i < len(counts)-1; i++ {
		for j := 0; j < len(counts)-i-1; j++ {
			if counts[j].count < counts[j+1].count {
				counts[j], counts[j+1] = counts[j+1], counts[j]
			}
		}
	}

	result := make([]string, 0, limit)
	for i := 0; i < len(counts) && i < limit; i++ {
		result = append(result, counts[i].id)
	}

	return result
}

// GetHourlyQueryStats returns hourly query statistics for a time range
func (mc *MetricsCollector) GetHourlyQueryStats(startTime, endTime time.Time) map[int64]int64 {
	mc.globalMutex.RLock()
	defer mc.globalMutex.RUnlock()

	result := make(map[int64]int64)

	startHour := startTime.Truncate(time.Hour).Unix()
	endHour := endTime.Truncate(time.Hour).Unix()

	for hour, count := range mc.globalMetrics.QueriesByHour {
		if hour >= startHour && hour <= endHour {
			result[hour] = count
		}
	}

	return result
}

// StartCleanupRoutine starts a background routine to clean up old metrics
func (mc *MetricsCollector) StartCleanupRoutine(ctx context.Context) {
	ticker := time.NewTicker(mc.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			mc.CleanupOldMetrics()
		}
	}
}

// ExportMetrics exports metrics in a format suitable for external monitoring
func (mc *MetricsCollector) ExportMetrics() map[string]interface{} {
	return map[string]interface{}{
		"global":  mc.GetGlobalMetrics(),
		"sources": mc.GetAllMetrics(),
		"summary": mc.GetMetricsSummary(),
	}
}

// Errors
var (
	ErrDataSourceMetricsNotFound = fmt.Errorf("data source metrics not found")
)
