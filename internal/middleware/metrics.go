package middleware

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// PrometheusMetrics holds all Prometheus metrics
type PrometheusMetrics struct {
	// HTTP request metrics
	HttpRequestsTotal    *prometheus.CounterVec
	HttpRequestDuration  *prometheus.HistogramVec
	HttpRequestSize      *prometheus.HistogramVec
	HttpResponseSize     *prometheus.HistogramVec

	// Database query metrics
	QueryTotal           *prometheus.CounterVec
	QueryDuration        *prometheus.HistogramVec
	QueryRowsRead        *prometheus.CounterVec
	QueryErrors          *prometheus.CounterVec

	// Connection pool metrics
	ConnectionPoolActive *prometheus.GaugeVec
	ConnectionPoolIdle   *prometheus.GaugeVec
	ConnectionPoolWait   *prometheus.HistogramVec

	// Data source health metrics
	DataSourceHealth     *prometheus.GaugeVec
	DataSourceUp         *prometheus.GaugeVec

	// Streaming metrics
	ActiveStreams        *prometheus.GaugeVec
	StreamRowsSent       *prometheus.CounterVec
	StreamDuration       *prometheus.HistogramVec

	// Token rotation metrics
	TokenRotations       *prometheus.CounterVec
	TokenRotationErrors  *prometheus.CounterVec
}

var (
	metrics *PrometheusMetrics
)

// InitMetrics initializes all Prometheus metrics
func InitMetrics() {
	metrics = &PrometheusMetrics{
		// HTTP request metrics
		HttpRequestsTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "nexus_http_requests_total",
				Help: "Total number of HTTP requests",
			},
			[]string{"method", "endpoint", "status"},
		),
		HttpRequestDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "nexus_http_request_duration_seconds",
				Help:    "HTTP request latency in seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"method", "endpoint"},
		),
		HttpRequestSize: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "nexus_http_request_size_bytes",
				Help:    "HTTP request size in bytes",
				Buckets: []float64{100, 1000, 10000, 100000, 1000000},
			},
			[]string{"method", "endpoint"},
		),
		HttpResponseSize: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "nexus_http_response_size_bytes",
				Help:    "HTTP response size in bytes",
				Buckets: []float64{100, 1000, 10000, 100000, 1000000},
			},
			[]string{"method", "endpoint"},
		),

		// Database query metrics
		QueryTotal: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "nexus_query_total",
				Help: "Total number of database queries",
			},
			[]string{"datasource_id", "database_type", "status"},
		),
		QueryDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "nexus_query_duration_seconds",
				Help:    "Database query execution time in seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"datasource_id", "database_type"},
		),
		QueryRowsRead: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "nexus_query_rows_read_total",
				Help: "Total number of rows read from queries",
			},
			[]string{"datasource_id", "database_type"},
		),
		QueryErrors: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "nexus_query_errors_total",
				Help: "Total number of query errors",
			},
			[]string{"datasource_id", "database_type", "error_type"},
		),

		// Connection pool metrics
		ConnectionPoolActive: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "nexus_connection_pool_active",
				Help: "Number of active connections in the pool",
			},
			[]string{"datasource_id", "database_type"},
		),
		ConnectionPoolIdle: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "nexus_connection_pool_idle",
				Help: "Number of idle connections in the pool",
			},
			[]string{"datasource_id", "database_type"},
		),
		ConnectionPoolWait: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "nexus_connection_pool_wait_seconds",
				Help:    "Time spent waiting for a connection from the pool",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"datasource_id", "database_type"},
		),

		// Data source health metrics
		DataSourceHealth: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "nexus_datasource_health",
				Help: "Data source health status (1=healthy, 0=unhealthy)",
			},
			[]string{"datasource_id", "database_type"},
		),
		DataSourceUp: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "nexus_datasource_up",
				Help: "Whether the data source is up (1=up, 0=down)",
			},
			[]string{"datasource_id", "database_type"},
		),

		// Streaming metrics
		ActiveStreams: promauto.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "nexus_streaming_active",
				Help: "Number of active streaming queries",
			},
			[]string{"datasource_id", "database_type"},
		),
		StreamRowsSent: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "nexus_streaming_rows_sent_total",
				Help: "Total number of rows sent via streaming",
			},
			[]string{"datasource_id", "database_type"},
		),
		StreamDuration: promauto.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "nexus_streaming_duration_seconds",
				Help:    "Streaming query duration in seconds",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"datasource_id", "database_type"},
		),

		// Token rotation metrics
		TokenRotations: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "nexus_token_rotations_total",
				Help: "Total number of token rotations",
			},
			[]string{"datasource_id", "database_type"},
		),
		TokenRotationErrors: promauto.NewCounterVec(
			prometheus.CounterOpts{
				Name: "nexus_token_rotation_errors_total",
				Help: "Total number of token rotation errors",
			},
			[]string{"datasource_id", "database_type"},
		),
	}
}

// GetMetrics returns the initialized metrics
func GetMetrics() *PrometheusMetrics {
	return metrics
}

// PrometheusMiddleware is a Gin middleware that records HTTP metrics
func PrometheusMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if metrics == nil {
			c.Next()
			return
		}

		// Start timer
		start := time.Now()

		// Process request
		c.Next()

		// Calculate metrics
		duration := time.Since(start).Seconds()
		status := strconv.Itoa(c.Writer.Status())
		method := c.Request.Method
		endpoint := c.FullPath()

		if endpoint == "" {
			endpoint = c.Request.URL.Path
		}

		// Record metrics
		metrics.HttpRequestsTotal.WithLabelValues(method, endpoint, status).Inc()
		metrics.HttpRequestDuration.WithLabelValues(method, endpoint).Observe(duration)

		// Record request size if available
		if c.Request.ContentLength > 0 {
			metrics.HttpRequestSize.WithLabelValues(method, endpoint).Observe(float64(c.Request.ContentLength))
		}

		// Record response size if available
		if c.Writer.Size() > 0 {
			metrics.HttpResponseSize.WithLabelValues(method, endpoint).Observe(float64(c.Writer.Size()))
		}
	}
}

// RecordQueryMetrics records query execution metrics
func RecordQueryMetrics(datasourceID, databaseType, status string, duration time.Duration, rowsRead int64) {
	if metrics == nil {
		return
	}

	metrics.QueryTotal.WithLabelValues(datasourceID, databaseType, status).Inc()
	metrics.QueryDuration.WithLabelValues(datasourceID, databaseType).Observe(duration.Seconds())

	if status == "success" && rowsRead > 0 {
		metrics.QueryRowsRead.WithLabelValues(datasourceID, databaseType).Add(float64(rowsRead))
	}
}

// RecordQueryError records a query error
func RecordQueryError(datasourceID, databaseType, errorType string) {
	if metrics == nil {
		return
	}

	metrics.QueryErrors.WithLabelValues(datasourceID, databaseType, errorType).Inc()
}

// UpdateConnectionPoolMetrics updates connection pool metrics
func UpdateConnectionPoolMetrics(datasourceID, databaseType string, active, idle int) {
	if metrics == nil {
		return
	}

	metrics.ConnectionPoolActive.WithLabelValues(datasourceID, databaseType).Set(float64(active))
	metrics.ConnectionPoolIdle.WithLabelValues(datasourceID, databaseType).Set(float64(idle))
}

// UpdateDataSourceHealth updates data source health metrics
func UpdateDataSourceHealth(datasourceID, databaseType string, healthy, up bool) {
	if metrics == nil {
		return
	}

	healthValue := 0.0
	if healthy {
		healthValue = 1.0
	}
	metrics.DataSourceHealth.WithLabelValues(datasourceID, databaseType).Set(healthValue)

	upValue := 0.0
	if up {
		upValue = 1.0
	}
	metrics.DataSourceUp.WithLabelValues(datasourceID, databaseType).Set(upValue)
}

// UpdateStreamingMetrics updates streaming metrics
func UpdateStreamingMetrics(datasourceID, databaseType string, activeStreams int, rowsSent int64, duration time.Duration) {
	if metrics == nil {
		return
	}

	metrics.ActiveStreams.WithLabelValues(datasourceID, databaseType).Set(float64(activeStreams))
	metrics.StreamRowsSent.WithLabelValues(datasourceID, databaseType).Add(float64(rowsSent))
	metrics.StreamDuration.WithLabelValues(datasourceID, databaseType).Observe(duration.Seconds())
}

// RecordTokenRotation records a token rotation event
func RecordTokenRotation(datasourceID, databaseType string, success bool) {
	if metrics == nil {
		return
	}

	if success {
		metrics.TokenRotations.WithLabelValues(datasourceID, databaseType).Inc()
	} else {
		metrics.TokenRotationErrors.WithLabelValues(datasourceID, databaseType).Inc()
	}
}
