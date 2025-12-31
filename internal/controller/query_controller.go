package controller

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"nexus-gateway/internal/model"
	"nexus-gateway/internal/service"
	"nexus-gateway/pkg/response"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
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
// @Description Executes a read-only SQL query against the specified data source.
// Supports two modes:
//   - Inline (default): returns rows up to `limit` in the response.
//   - Streaming: when supported by the backend, the service will open a server-side cursor
//     and return the first batch of rows plus a `nextUri` which can be used to fetch
//     subsequent batches from the `/fetch` endpoint. Clients should follow `nextUri`
//     to continue streaming results without re-running the query.
//
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

// FetchQuery godoc
// @Summary Execute a batch SQL query
// @Description Executes a SQL query and returns batch data with pagination support.
// This endpoint will attempt to open a streaming cursor on the backend when possible
// and return the first batch of rows. If streaming is established, the response
// includes a `nextUri` for continuing the stream via `/fetch/{query_id}/{slug}/{token}`.
// @Tags queries
// @Accept json
// @Produce json
// @Param request body model.FetchQueryRequest true "Fetch query request"
// @Success 200 {object} response.StandardResponse{data=model.FetchQueryResponse}
// @Failure 400 {object} response.StandardResponse
// @Failure 422 {object} response.StandardResponse
// @Failure 500 {object} response.StandardResponse
// @Router /api/v1/fetch [post]
func (qc *QueryController) FetchQuery(c *gin.Context) {
	correlationID := qc.getCorrelationID(c)

	var req model.FetchQueryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, response.ErrorResponse(
			"INVALID_REQUEST",
			"Invalid request body: "+err.Error(),
			"",
			correlationID,
		))
		return
	}

	// Apply defaults
	if req.BatchSize <= 0 {
		req.BatchSize = 10000
	}
	if req.BatchSize > 10000 {
		req.BatchSize = 10000
	}
	if req.Timeout <= 0 {
		req.Timeout = 60
	}
	if req.Timeout > 1800 {
		req.Timeout = 1800
	}

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
	var cancel context.CancelFunc
	if req.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.Timeout)*time.Second)
		defer cancel()
	}

	// Execute fetch query
	result, err := qc.queryService.FetchQuery(ctx, &req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.ErrorResponse(
			"FETCH_ERROR",
			"Failed to execute fetch query: "+err.Error(),
			"",
			correlationID,
		))
		return
	}

	// Return response
	c.JSON(http.StatusOK, response.SuccessResponse(result, correlationID))
}

// FetchNextBatch godoc
// @Summary Fetch next batch of data
// @Description Fetches the next batch of data using the query ID, slug, and token.
// This endpoint retrieves subsequent batches for streaming queries (created by
// either `/query` when it returned a `nextUri` or `/fetch`), and also supports
// offset-based continuation for legacy sessions that fall back to LIMIT/OFFSET.
// @Tags queries
// @Accept json
// @Produce json
// @Param query_id path string true "Query ID"
// @Param slug path string true "Query slug"
// @Param token path string true "Query token"
// @Param batch_size query int false "Batch size (1-10000, default 10000)"
// @Success 200 {object} response.StandardResponse{data=model.FetchQueryResponse}
// @Failure 400 {object} response.StandardResponse
// @Failure 404 {object} response.StandardResponse
// @Failure 500 {object} response.StandardResponse
// @Router /api/v1/fetch/{query_id}/{slug}/{token} [get]
func (qc *QueryController) FetchNextBatch(c *gin.Context) {
	correlationID := qc.getCorrelationID(c)

	// Extract path parameters
	queryID := c.Param("query_id")
	slug := c.Param("slug")
	token := c.Param("token")

	if queryID == "" || slug == "" || token == "" {
		c.JSON(http.StatusBadRequest, response.ErrorResponse(
			"INVALID_PARAMETERS",
			"Query ID, slug, and token are required",
			"",
			correlationID,
		))
		return
	}

	// Parse batch size
	batchSize := 10000
	if bs := c.Query("batch_size"); bs != "" {
		if size, err := strconv.Atoi(bs); err == nil && size > 0 && size <= 10000 {
			batchSize = size
		}
	}

	// Create context
	ctx := c.Request.Context()

	// Fetch next batch
	result, err := qc.queryService.FetchNextBatch(ctx, queryID, slug, token, batchSize)
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.ErrorResponse(
			"FETCH_ERROR",
			"Failed to fetch next batch: "+err.Error(),
			"",
			correlationID,
		))
		return
	}

	c.JSON(http.StatusOK, response.SuccessResponse(result, correlationID))
}

// InternalFetchQuery handles internal fetch queries with account headers
func (qc *QueryController) InternalFetchQuery(c *gin.Context) {
	correlationID := qc.getCorrelationID(c)

	// Extract account headers
	accountID := c.GetHeader("x-account-id")
	accountType := c.GetHeader("x-account-type")

	if accountID == "" || accountType == "" {
		c.JSON(http.StatusBadRequest, response.ErrorResponse(
			"MISSING_ACCOUNT_HEADERS",
			"x-account-id and x-account-type headers are required",
			"",
			correlationID,
		))
		return
	}

	var req model.FetchQueryRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, response.ErrorResponse(
			"INVALID_REQUEST",
			"Invalid request body: "+err.Error(),
			"",
			correlationID,
		))
		return
	}

	// Apply defaults
	if req.BatchSize <= 0 {
		req.BatchSize = 10000
	}
	if req.BatchSize > 10000 {
		req.BatchSize = 10000
	}
	if req.Timeout <= 0 {
		req.Timeout = 60
	}
	if req.Timeout > 1800 {
		req.Timeout = 1800
	}

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
	var cancel context.CancelFunc
	if req.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.Timeout)*time.Second)
		defer cancel()
	}

	// Execute fetch query
	result, err := qc.queryService.FetchQuery(ctx, &req)
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.ErrorResponse(
			"FETCH_ERROR",
			"Failed to execute fetch query: "+err.Error(),
			"",
			correlationID,
		))
		return
	}

	// Add account information to response (for internal tracking)
	if result != nil {
		// Note: We don't modify the response structure for now
		// In production, you might want to add account metadata
	}

	// Return response
	c.JSON(http.StatusOK, response.SuccessResponse(result, correlationID))
}

// InternalFetchNextBatch handles internal fetch next batch requests with account headers
func (qc *QueryController) InternalFetchNextBatch(c *gin.Context) {
	correlationID := qc.getCorrelationID(c)

	// Extract account headers
	accountID := c.GetHeader("x-account-id")
	accountType := c.GetHeader("x-account-type")

	if accountID == "" || accountType == "" {
		c.JSON(http.StatusBadRequest, response.ErrorResponse(
			"MISSING_ACCOUNT_HEADERS",
			"x-account-id and x-account-type headers are required",
			"",
			correlationID,
		))
		return
	}

	// Extract path parameters
	queryID := c.Param("query_id")
	slug := c.Param("slug")
	token := c.Param("token")

	if queryID == "" || slug == "" || token == "" {
		c.JSON(http.StatusBadRequest, response.ErrorResponse(
			"INVALID_PARAMETERS",
			"Query ID, slug, and token are required",
			"",
			correlationID,
		))
		return
	}

	// Parse batch size
	batchSize := 10000
	if bs := c.Query("batch_size"); bs != "" {
		if size, err := strconv.Atoi(bs); err == nil && size > 0 && size <= 10000 {
			batchSize = size
		}
	}

	// Create context
	ctx := c.Request.Context()

	// Fetch next batch
	result, err := qc.queryService.FetchNextBatch(ctx, queryID, slug, token, batchSize)
	if err != nil {
		c.JSON(http.StatusInternalServerError, response.ErrorResponse(
			"FETCH_ERROR",
			"Failed to fetch next batch: "+err.Error(),
			"",
			correlationID,
		))
		return
	}

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
			"valid":   false,
			"error":   err.Error(),
			"sql":     req.SQL,
			"queryId": qc.generateQueryID(),
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

func (qc *QueryController) generateSlug() string {
	// Generate a slug for query identification
	return uuid.New().String()[:8]
}

func (qc *QueryController) generateToken() string {
	// Generate a token for authorization
	return uuid.New().String()
}
