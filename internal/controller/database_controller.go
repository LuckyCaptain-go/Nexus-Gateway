package controller

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"nexus-gateway/internal/database"
	"nexus-gateway/internal/model"
	"nexus-gateway/internal/repository"
	"nexus-gateway/pkg/response"
)

type DatabaseController struct {
	datasourceRepo repository.DataSourceRepository
	healthChecker  *database.HealthChecker
}

func NewDatabaseController(datasourceRepo repository.DataSourceRepository, healthChecker *database.HealthChecker) *DatabaseController {
	return &DatabaseController{
		datasourceRepo: datasourceRepo,
		healthChecker:  healthChecker,
	}
}

// GetDatabaseTypes godoc
// @Summary Get supported database types
// @Description Returns a list of supported database types with their information
// @Tags database
// @Produce json
// @Success 200 {object} response.StandardResponse
// @Router /api/v1/database/types [get]
func (dc *DatabaseController) GetDatabaseTypes(c *gin.Context) {
	correlationID := dc.getCorrelationID(c)

	driverInfo := dc.healthChecker.GetDriverInfo()

	c.JSON(http.StatusOK, response.SuccessResponse(driverInfo, correlationID))
}

// TestDataSourceConnection godoc
// @Summary Test data source connection
// @Description Tests connectivity to a data source without adding it to the pool
// @Tags database
// @Accept json
// @Produce json
// @Param request body model.DataSourceConfig true "Data source configuration"
// @Param type query string true "Database type (mysql, mariadb, postgresql, oracle)"
// @Success 200 {object} response.StandardResponse{data=database.HealthCheckResult}
// @Failure 400 {object} response.StandardResponse
// @Router /api/v1/database/test-connection [post]
func (dc *DatabaseController) TestDataSourceConnection(c *gin.Context) {
	correlationID := dc.getCorrelationID(c)

	// Get database type from query parameter
	dbTypeStr := c.Query("type")
	if dbTypeStr == "" {
		c.JSON(http.StatusBadRequest, response.ErrorResponse(
			"MISSING_TYPE",
			"Database type is required",
			"",
			correlationID,
		))
		return
	}

	// Validate database type
	dbType := model.DatabaseType(dbTypeStr)
	switch dbType {
	case model.DatabaseTypeMySQL, model.DatabaseTypeMariaDB, model.DatabaseTypePostgreSQL, model.DatabaseTypeOracle:
		// Valid database types
	default:
		c.JSON(http.StatusBadRequest, response.ErrorResponse(
			"INVALID_TYPE",
			"Invalid database type. Supported types: mysql, mariadb, postgresql, oracle",
			"",
			correlationID,
		))
		return
	}

	var config model.DataSourceConfig
	if err := c.ShouldBindJSON(&config); err != nil {
		c.JSON(http.StatusBadRequest, response.ErrorResponse(
			"INVALID_REQUEST",
			"Invalid request body: "+err.Error(),
			"",
			correlationID,
		))
		return
	}

	// Validate configuration
	if err := dc.healthChecker.ValidateDataSourceConfiguration(&config, dbType); err != nil {
		c.JSON(http.StatusBadRequest, response.ErrorResponse(
			"INVALID_CONFIG",
			"Invalid configuration: "+err.Error(),
			"",
			correlationID,
		))
		return
	}

	// Test connectivity
	result, err := dc.healthChecker.CheckDataSourceConnectivity(c.Request.Context(), &config, dbType)
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.ErrorResponse(
			"CONNECTION_TEST_FAILED",
			"Failed to test connection: "+err.Error(),
			"",
			correlationID,
		))
		return
	}

	c.JSON(http.StatusOK, response.SuccessResponse(result, correlationID))
}

// ValidateDataSourceConfig godoc
// @Summary Validate data source configuration
// @Description Validates a data source configuration without testing the connection
// @Tags database
// @Accept json
// @Produce json
// @Param request body ValidateConfigRequest true "Validation request"
// @Success 200 {object} response.StandardResponse
// @Failure 400 {object} response.StandardResponse
// @Router /api/v1/database/validate-config [post]
func (dc *DatabaseController) ValidateDataSourceConfig(c *gin.Context) {
	correlationID := dc.getCorrelationID(c)

	var req ValidateConfigRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, response.ErrorResponse(
			"INVALID_REQUEST",
			"Invalid request body: "+err.Error(),
			"",
			correlationID,
		))
		return
	}

	// Validate database type
	dbType := model.DatabaseType(req.Type)
	switch dbType {
	case model.DatabaseTypeMySQL, model.DatabaseTypeMariaDB, model.DatabaseTypePostgreSQL, model.DatabaseTypeOracle:
		// Valid database types
	default:
		c.JSON(http.StatusBadRequest, response.ErrorResponse(
			"INVALID_TYPE",
			"Invalid database type. Supported types: mysql, mariadb, postgresql, oracle",
			"",
			correlationID,
		))
		return
	}

	// Validate configuration
	if err := dc.healthChecker.ValidateDataSourceConfiguration(&req.Config, dbType); err != nil {
		c.JSON(http.StatusOK, response.SuccessResponse(map[string]interface{}{
			"valid":  false,
			"errors": []string{err.Error()},
		}, correlationID))
		return
	}

	c.JSON(http.StatusOK, response.SuccessResponse(map[string]interface{}{
		"valid":    true,
		"warnings": dc.getConfigWarnings(&req.Config, dbType),
	}, correlationID))
}

// GetConnectionStats godoc
// @Summary Get connection pool statistics
// @Description Returns statistics for all connection pools
// @Tags database
// @Produce json
// @Success 200 {object} response.StandardResponse
// @Router /api/v1/database/connections/stats [get]
func (dc *DatabaseController) GetConnectionStats(c *gin.Context) {
	correlationID := dc.getCorrelationID(c)

	// This would need access to the connection pool
	// For now, return a placeholder response
	c.JSON(http.StatusOK, response.SuccessResponse(map[string]interface{}{
		"message":           "Connection pool statistics not yet implemented",
		"totalConnections":  0,
		"activeConnections": 0,
		"idleConnections":   0,
	}, correlationID))
}

// GetDatabaseHealth godoc
// @Summary Get database health status
// @Description Returns health status of all database connections
// @Tags database
// @Produce json
// @Success 200 {object} response.StandardResponse{data=database.DatabaseHealthSummary}
// @Router /api/v1/database/health [get]
func (dc *DatabaseController) GetDatabaseHealth(c *gin.Context) {
	correlationID := dc.getCorrelationID(c)

	// This would need access to the health checker with connection pool
	// For now, return a placeholder response
	c.JSON(http.StatusOK, response.SuccessResponse(map[string]interface{}{
		"message":              "Database health check not yet implemented",
		"healthyConnections":   0,
		"unhealthyConnections": 0,
		"totalConnections":     0,
		"checkedAt":            "2025-12-22T10:00:00Z",
	}, correlationID))
}

// GetDatabaseCompatibility godoc
// @Summary Get database compatibility information
// @Description Returns compatibility information for different database types
// @Tags database
// @Produce json
// @Success 200 {object} response.StandardResponse
// @Router /api/v1/database/compatibility [get]
func (dc *DatabaseController) GetDatabaseCompatibility(c *gin.Context) {
	correlationID := dc.getCorrelationID(c)

	compatibility := map[string]DatabaseCompatibility{
		"mysql": {
			Supported:   true,
			Version:     "5.7+",
			Features:    []string{"SELECT", "JOIN", "GROUP BY", "ORDER BY", "LIMIT"},
			Limitations: []string{"No DML/DDL operations", "No stored procedures"},
			Recommended: true,
		},
		"mariadb": {
			Supported:   true,
			Version:     "10.2+",
			Features:    []string{"SELECT", "JOIN", "GROUP BY", "ORDER BY", "LIMIT"},
			Limitations: []string{"No DML/DDL operations", "No stored procedures"},
			Recommended: true,
		},
		"postgresql": {
			Supported:   true,
			Version:     "9.6+",
			Features:    []string{"SELECT", "JOIN", "GROUP BY", "ORDER BY", "LIMIT", "Window functions"},
			Limitations: []string{"No DML/DDL operations", "No stored procedures"},
			Recommended: true,
		},
		"oracle": {
			Supported:   true,
			Version:     "12c+",
			Features:    []string{"SELECT", "JOIN", "GROUP BY", "ORDER BY"},
			Limitations: []string{"No DML/DDL operations", "No stored procedures", "Limited pagination support"},
			Recommended: true,
		},
	}

	c.JSON(http.StatusOK, response.SuccessResponse(compatibility, correlationID))
}

// Request/Response types

type ValidateConfigRequest struct {
	Type   string                 `json:"type" validate:"required"`
	Config model.DataSourceConfig `json:"config" validate:"required"`
}

type DatabaseCompatibility struct {
	Supported   bool     `json:"supported"`
	Version     string   `json:"version"`
	Features    []string `json:"features"`
	Limitations []string `json:"limitations"`
	Recommended bool     `json:"recommended"`
}

// Helper methods

func (dc *DatabaseController) getCorrelationID(c *gin.Context) string {
	if correlationID, exists := c.Get("correlation_id"); exists {
		if id, ok := correlationID.(string); ok {
			return id
		}
	}
	return ""
}

func (dc *DatabaseController) getConfigWarnings(config *model.DataSourceConfig, dbType model.DatabaseType) []string {
	warnings := []string{}

	// Check for potential configuration issues
	if config.MaxPoolSize > 50 {
		warnings = append(warnings, "Large connection pool size may impact performance")
	}

	if config.Timeout > 60 {
		warnings = append(warnings, "Long timeout may mask performance issues")
	}

	if !config.SSL && dbType != model.DatabaseTypeOracle {
		warnings = append(warnings, "SSL is disabled - connection will not be encrypted")
	}

	return warnings
}
