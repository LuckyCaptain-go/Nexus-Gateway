package unifiedservice

import (
	"context"
	"nexus-gateway/internal/config"
	"nexus-gateway/internal/model"
	service "nexus-gateway/internal/service"
)

// QueryService interface that UnifiedQueryService implements
type QueryService interface {
	ValidateQuery(ctx context.Context, req *model.QueryRequest) error
	GetQueryStats(ctx context.Context) (*model.QueryStats, error)
	FetchQuery(ctx context.Context, req *model.FetchQueryRequest) (*model.FetchQueryResponse, error)
	FetchNextBatch(ctx context.Context, queryID, slug, token string, batchSize int) (*model.FetchQueryResponse, error)
}

// UnifiedQueryService provides a unified interface that can use either pagination or streaming
// based on configuration, but always returns the same response format
type UnifiedQueryService struct {
	paginationService service.QueryService // The original QueryService implementation
	streamingService  service.StreamingQueryService
	config            *config.Config
}

// NewUnifiedQueryService creates a new unified query service that chooses between
// pagination and streaming based on configuration
func NewUnifiedQueryService(
	paginationService service.QueryService,
	streamingService service.StreamingQueryService,
	config *config.Config,
) *UnifiedQueryService {
	return &UnifiedQueryService{
		paginationService: paginationService,
		streamingService:  streamingService,
		config:            config,
	}
}

// ValidateQuery validates the query using the pagination service's validator
func (uqs *UnifiedQueryService) ValidateQuery(ctx context.Context, req *model.QueryRequest) error {
	return uqs.paginationService.ValidateQuery(ctx, req)
}

// GetQueryStats returns query statistics
func (uqs *UnifiedQueryService) GetQueryStats(ctx context.Context) (*model.QueryStats, error) {
	return uqs.paginationService.GetQueryStats(ctx)
}

// FetchQuery executes a fetch query using the pagination service
func (uqs *UnifiedQueryService) FetchQuery(ctx context.Context, req *model.FetchQueryRequest) (*model.FetchQueryResponse, error) {
	// Determine execution mode based on configuration
	executionMode := uqs.getExecutionMode()

	switch executionMode {
	case "streaming":
		return uqs.streamingService.FetchWithStreaming(ctx, req)
	case "pagination":
		return uqs.paginationService.FetchQuery(ctx, req)
	case "auto":
		// Default to pagination for compatibility
		return uqs.paginationService.FetchQuery(ctx, req)
	default:
		// Default to pagination
		return uqs.paginationService.FetchQuery(ctx, req)
	}
}

// FetchNextBatch fetches the next batch using the pagination service
func (uqs *UnifiedQueryService) FetchNextBatch(ctx context.Context, queryID, slug, token string, batchSize int) (*model.FetchQueryResponse, error) {
	// Determine execution mode based on configuration
	executionMode := uqs.getExecutionMode()

	switch executionMode {
	case "streaming":
		return uqs.streamingService.FetchNextBatchWithStreaming(ctx, queryID, slug, token, batchSize)
	case "pagination":
		return uqs.paginationService.FetchNextBatch(ctx, queryID, slug, token, batchSize)
	case "auto":
		// Default to pagination for compatibility
		return uqs.paginationService.FetchNextBatch(ctx, queryID, slug, token, batchSize)
	default:
		// Default to pagination
		return uqs.paginationService.FetchNextBatch(ctx, queryID, slug, token, batchSize)
	}
}

// getExecutionMode determines the execution mode based on configuration
func (uqs *UnifiedQueryService) getExecutionMode() string {
	if uqs.config != nil && uqs.config.Query.ExecutionMode != "" {
		return uqs.config.Query.ExecutionMode
	}
	// Default to auto mode
	return "auto"
}
