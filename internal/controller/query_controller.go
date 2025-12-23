package controller

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
	"nexus-gateway/internal/model"
	"nexus-gateway/internal/service"
	"nexus-gateway/pkg/response"
)

type QueryController struct {
	queryService service.QueryService
	validator    *validator.Validate
}

func NewQueryController(queryService service.QueryService) *QueryController {
	return &QueryController{
		queryService: queryService,
		validator:    validator.New(),
	}
}

// ExecuteQuery godoc
// @Summary Execute a SQL query
// @Description Executes a read-only SQL query against the specified data source
// @Tags queries
// @Accept json
// @Produce json
// @Param request body model.QueryRequest true "Query execution request"
// @Success 200 {object} response.StandardResponse{data=model.QueryResponse}
// @Failure 400 {object} response.StandardResponse
// @Failure 404 {object} response.StandardResponse
// @Failure 422 {object} response.StandardResponse
// @Failure 500 {object} response.StandardResponse
// @Router /api/v1/query [post]
func (qc *QueryController) ExecuteQuery(c *gin.Context) {
	// Get correlation ID
	correlationID := qc.getCorrelationID(c)

	var req model.QueryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, response.ErrorResponse(
			"INVALID_REQUEST",
			"Invalid request body: "+err.Error(),
			"",
			correlationID,
		))
		return
	}

	// Apply defaults to request
	req.ApplyDefaults()

	// Validate request
	if err := qc.validator.Struct(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, response.ErrorResponse(
			"VALIDATION_ERROR",
			err.Error(),
			"",
			correlationID,
		))
		return
	}

	// Create context with timeout
	ctx := c.Request.Context()
	if req.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.Timeout)*time.Second)
		defer cancel()
	}

	// Execute query
	result, err := qc.queryService.ExecuteQuery(ctx, &req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.ErrorResponse(
			"QUERY_EXECUTION_ERROR",
			"Failed to execute query: "+err.Error(),
			"",
			correlationID,
		))
		return
	}

	// Return response
	c.JSON(http.StatusOK, response.SuccessResponse(result, correlationID))
}

// ValidateQuery godoc
// @Summary Validate a SQL query
// @Description Validates a SQL query without executing it
// @Tags queries
// @Accept json
// @Produce json
// @Param request body model.QueryRequest true "Query validation request"
// @Success 200 {object} response.StandardResponse
// @Failure 400 {object} response.StandardResponse
// @Failure 422 {object} response.StandardResponse
// @Router /api/v1/query/validate [post]
func (qc *QueryController) ValidateQuery(c *gin.Context) {
	correlationID := qc.getCorrelationID(c)

	var req model.QueryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, response.ErrorResponse(
			"INVALID_REQUEST",
			"Invalid request body: "+err.Error(),
			"",
			correlationID,
		))
		return
	}

	// Apply defaults to request
	req.ApplyDefaults()

	// Validate request structure
	if err := qc.validator.Struct(&req); err != nil {
		c.JSON(http.StatusUnprocessableEntity, response.ErrorResponse(
			"VALIDATION_ERROR",
			err.Error(),
			"",
			correlationID,
		))
		return
	}

	// Validate query
	err := qc.queryService.ValidateQuery(c.Request.Context(), &req)
	if err != nil {
		c.JSON(http.StatusOK, response.SuccessResponse(map[string]interface{}{
			"valid":    false,
			"error":    err.Error(),
			"sql":      req.SQL,
			"queryId":  qc.generateQueryID(),
		}, correlationID))
		return
	}

	c.JSON(http.StatusOK, response.SuccessResponse(map[string]interface{}{
		"valid":   true,
		"sql":     req.SQL,
		"queryId": qc.generateQueryID(),
	}, correlationID))
}

// GetQueryStats godoc
// @Summary Get query statistics
// @Description Returns statistics about query executions
// @Tags queries
// @Produce json
// @Success 200 {object} response.StandardResponse{data=model.QueryStats}
// @Router /api/v1/query/stats [get]
func (qc *QueryController) GetQueryStats(c *gin.Context) {
	correlationID := qc.getCorrelationID(c)

	stats, err := qc.queryService.GetQueryStats(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.ErrorResponse(
			"STATS_ERROR",
			"Failed to get query statistics: "+err.Error(),
			"",
			correlationID,
		))
		return
	}

	c.JSON(http.StatusOK, response.SuccessResponse(stats, correlationID))
}

// GetQueryHistory godoc
// @Summary Get recent query history
// @Description Returns a list of recent queries (placeholder for now)
// @Tags queries
// @Produce json
// @Param limit query int false "Maximum number of items to return (default: 100, max: 1000)"
// @Param offset query int false "Number of items to skip (default: 0)"
// @Success 200 {object} response.StandardResponse
// @Router /api/v1/query/history [get]
func (qc *QueryController) GetQueryHistory(c *gin.Context) {
	correlationID := qc.getCorrelationID(c)

	// Parse query parameters
	limit := 100
	offset := 0

	if limitStr := c.Query("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
			if limit > 1000 {
				limit = 1000
			}
		}
	}

	if offsetStr := c.Query("offset"); offsetStr != "" {
		if o, err := strconv.Atoi(offsetStr); err == nil && o >= 0 {
			offset = o
		}
	}

	// For now, return empty history
	// In a real implementation, you would store query history in a database
	history := map[string]interface{}{
		"queries": []interface{}{},
		"total":   0,
		"limit":   limit,
		"offset":  offset,
	}

	c.JSON(http.StatusOK, response.SuccessResponse(history, correlationID))
}

// Helper methods

func (qc *QueryController) getCorrelationID(c *gin.Context) string {
	if correlationID, exists := c.Get("correlation_id"); exists {
		if id, ok := correlationID.(string); ok {
			return id
		}
	}
	return ""
}

func (qc *QueryController) generateQueryID() string {
	// Generate a simple query ID for tracking
	return strconv.FormatInt(time.Now().UnixNano(), 36)
}