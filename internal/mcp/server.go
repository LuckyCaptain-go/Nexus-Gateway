package mcp

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"nexus-gateway/internal/model"
	"nexus-gateway/internal/unified_service"
	"nexus-gateway/internal/repository"
)

// MCPServer represents the MCP server that exposes Nexus-Gateway functionality
type MCPServer struct {
	unifiedQueryService unifiedservice.UnifiedQueryService
	datasourceRepo      repository.DataSourceRepository
	server              *server.MCPServer
}

// NewMCPServer creates a new MCP server instance
func NewMCPServer(unifiedQueryService unifiedservice.UnifiedQueryService, datasourceRepo repository.DataSourceRepository) *MCPServer {
	s := server.NewMCPServer(
		"Nexus-Gateway MCP Server",
		"1.0.0",
	)

	mcpServer := &MCPServer{
		unifiedQueryService: unifiedQueryService,
		datasourceRepo:      datasourceRepo,
		server:              s,
	}

	// Register tools that will be available to LLMs
	mcpServer.registerTools()

	return mcpServer
}

// registerTools registers all available tools with the MCP server
func (m *MCPServer) registerTools() {
	// Tool to list available data sources
	listDataSourcesTool := mcp.NewTool("list_data_sources", 
		mcp.WithDescription("List all configured data sources in Nexus-Gateway"))
	m.server.AddTool(listDataSourcesTool, m.handleListDataSources)

	// Tool to execute SQL queries against data sources
	executeSQLQueryTool := mcp.NewTool("execute_sql_query",
		mcp.WithDescription("Execute a SQL query against a specified data source"),
		mcp.WithString("datasource_id", mcp.Required(), mcp.Description("The ID of the data source to query")),
		mcp.WithString("sql", mcp.Required(), mcp.Description("The SQL query to execute")),
		mcp.WithNumber("batch_size", mcp.Description("Number of records to return per batch, default 10000")),
		mcp.WithNumber("timeout", mcp.Description("Query timeout in seconds, default 60")))
	m.server.AddTool(executeSQLQueryTool, m.handleExecuteSQLQuery)

	// Tool to get information about a specific data source
	getDataSourceInfoTool := mcp.NewTool("get_data_source_info",
		mcp.WithDescription("Get detailed information about a specific data source"),
		mcp.WithString("datasource_id", mcp.Required(), mcp.Description("The ID of the data source to get info for")))
	m.server.AddTool(getDataSourceInfoTool, m.handleGetDataSourceInfo)

	// Tool to validate a SQL query against a data source
	validateSQLQueryTool := mcp.NewTool("validate_sql_query",
		mcp.WithDescription("Validate a SQL query without executing it"),
		mcp.WithString("datasource_id", mcp.Required(), mcp.Description("The ID of the data source to validate against")),
		mcp.WithString("sql", mcp.Required(), mcp.Description("The SQL query to validate")))
	m.server.AddTool(validateSQLQueryTool, m.handleValidateSQLQuery)

	// Tool to list tables in a specific data source
	listTablesTool := mcp.NewTool("list_tables",
		mcp.WithDescription("List all tables in a specific data source"),
		mcp.WithString("datasource_id", mcp.Required(), mcp.Description("The ID of the data source to list tables from")))
	m.server.AddTool(listTablesTool, m.handleListTables)
}

// handleListDataSources handles the list_data_sources tool call
func (m *MCPServer) handleListDataSources(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	// Get all data sources from the repository
	// Using empty status filter and large limit to get all data sources
	dataSources, _, err := m.datasourceRepo.GetAll(ctx, "", 1000, 0) // Get up to 1000 data sources
	if err != nil {
		return nil, fmt.Errorf("failed to list data sources: %w", err)
	}

	var result []map[string]interface{}
	for _, ds := range dataSources {
		dsMap := map[string]interface{}{
			"id":          ds.ID,
			"name":        ds.Name,
			"type":        ds.Type,
			"status":      ds.Status, // Use Status instead of Description/Enabled
			"host":        ds.Config.Host,
			"port":        ds.Config.Port,
			"database":    ds.Config.Database,
			"created_at":  ds.CreatedAt,
			"updated_at":  ds.UpdatedAt,
		}
		result = append(result, dsMap)
	}

	response := map[string]interface{}{
		"data_sources": result,
		"count":        len(result),
	}

	jsonResp, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal response: %w", err)
	}

	return &mcp.CallToolResult{
		Content: []mcp.Content{
			mcp.NewTextContent(string(jsonResp)),
		},
	}, nil
}

// handleExecuteSQLQuery handles the execute_sql_query tool call
func (m *MCPServer) handleExecuteSQLQuery(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	// Extract parameters from the request
	datasourceID := mcp.ParseString(request, "datasource_id", "")
	sql := mcp.ParseString(request, "sql", "")
	batchSize := int(mcp.ParseInt(request, "batch_size", 10000))
	timeout := int(mcp.ParseInt(request, "timeout", 60))

	// Prepare the fetch query request
	req := &model.FetchQueryRequest{
		DataSourceID: datasourceID,
		SQL:          sql,
		BatchSize:    batchSize,
		Timeout:      timeout,
	}

	// Execute the query using the unified query service
	result, err := m.unifiedQueryService.FetchQuery(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	// Convert result to JSON for response
	jsonResp, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal response: %w", err)
	}

	return &mcp.CallToolResult{
		Content: []mcp.Content{
			mcp.NewTextContent(string(jsonResp)),
		},
	}, nil
}

// handleGetDataSourceInfo handles the get_data_source_info tool call
func (m *MCPServer) handleGetDataSourceInfo(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	// Extract parameters from the request
	datasourceID := mcp.ParseString(request, "datasource_id", "")

	// Get the data source from the repository
	dataSource, err := m.datasourceRepo.GetByID(ctx, datasourceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get data source: %w", err)
	}

	info := map[string]interface{}{
		"id":          dataSource.ID,
		"name":        dataSource.Name,
		"type":        dataSource.Type,
		"status":      dataSource.Status, // Use Status instead of Description/Enabled
		"config":      dataSource.Config,
		"created_at":  dataSource.CreatedAt,
		"updated_at":  dataSource.UpdatedAt,
	}

	jsonResp, err := json.Marshal(info)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal response: %w", err)
	}

	return &mcp.CallToolResult{
		Content: []mcp.Content{
			mcp.NewTextContent(string(jsonResp)),
		},
	}, nil
}

// handleValidateSQLQuery handles the validate_sql_query tool call
func (m *MCPServer) handleValidateSQLQuery(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	// Extract parameters from the request
	datasourceID := mcp.ParseString(request, "datasource_id", "")
	sql := mcp.ParseString(request, "sql", "")

	// Create a basic query request for validation
	req := &model.QueryRequest{
		DataSourceID: datasourceID,
		SQL:          sql,
	}

	// Validate the query using the unified query service
	err := m.unifiedQueryService.ValidateQuery(ctx, req)
	if err != nil {
		response := map[string]interface{}{
			"valid": false,
			"error": err.Error(),
		}

		jsonResp, err := json.Marshal(response)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal response: %w", err)
		}

		return &mcp.CallToolResult{
			Content: []mcp.Content{
				mcp.NewTextContent(string(jsonResp)),
			},
		}, nil
	}

	response := map[string]interface{}{
		"valid": true,
		"sql":   sql,
	}

	jsonResp, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal response: %w", err)
	}

	return &mcp.CallToolResult{
		Content: []mcp.Content{
			mcp.NewTextContent(string(jsonResp)),
		},
	}, nil
}

// handleListTables handles the list_tables tool call
func (m *MCPServer) handleListTables(ctx context.Context, request mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	// Extract parameters from the request
	datasourceID := mcp.ParseString(request, "datasource_id", "")

	// First, get the data source to determine its type
	dataSource, err := m.datasourceRepo.GetByID(ctx, datasourceID)
	if err != nil {
		return nil, fmt.Errorf("failed to get data source: %w", err)
	}

	// For now, we'll execute a query to list tables based on the database type
	// This is a simplified approach - in a real implementation, you'd want more robust table discovery
	var sql string
	dbType := dataSource.Type
	switch dbType {
	case "mysql", "mariadb":
		sql = "SHOW TABLES"
	case "postgres", "postgresql":
		sql = "SELECT tablename FROM pg_tables WHERE schemaname='public'"
	case "oracle":
		sql = "SELECT table_name FROM user_tables"
	case "sqlserver":
		sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"
	default:
		// Generic approach - try to use information_schema which is common among many databases
		sql = "SELECT table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'sys')"
	}

	req := &model.FetchQueryRequest{
		DataSourceID: datasourceID,
		SQL:          sql,
		BatchSize:    1000, // Limit the number of tables returned
		Timeout:      30,
	}

	result, err := m.unifiedQueryService.FetchQuery(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}

	// Convert result to JSON for response
	jsonResp, err := json.Marshal(result)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal response: %w", err)
	}

	return &mcp.CallToolResult{
		Content: []mcp.Content{
			mcp.NewTextContent(string(jsonResp)),
		},
	}, nil
}

// StartStdio starts the MCP server using stdio transport
func (m *MCPServer) StartStdio() error {
	return server.ServeStdio(m.server)
}

// Stop stops the MCP server
func (m *MCPServer) Stop() {
	// MCP server doesn't expose a stop method in the current API
	// This is a placeholder for future implementation
}