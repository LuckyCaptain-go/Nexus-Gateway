# Nexus-Gateway as MCP Server

This document explains how to run Nexus-Gateway as a Model Context Protocol (MCP) Server.

## What is MCP?

The Model Context Protocol (MCP) is a standard protocol developed by Anthropic that enables large language models (LLMs) to connect to external data sources and tools. By running Nexus-Gateway as an MCP Server, you can expose your database connections and query capabilities to AI models.

## Configuration

To run Nexus-Gateway as an MCP Server, update the configuration in `configs/config.yaml`:

```yaml
mcp:
  enabled: true      # Set to true to run as MCP server (default: false)
  transport: "stdio" # Options: stdio (default), http, sse
  port: "8090"       # Port for HTTP transport (not used for stdio)
  host: "0.0.0.0"    # Host for HTTP transport (not used for stdio)
```

## Running as MCP Server

### 1. Using stdio transport (recommended for Claude Desktop)

First, set the MCP configuration in `configs/config.yaml`:

```yaml
mcp:
  enabled: true
  transport: "stdio"
```

Then run the server:

```bash
go run ./cmd/server/main.go
```

### 2. Using HTTP transport

First, set the MCP configuration in `configs/config.yaml`:

```yaml
mcp:
  enabled: true
  transport: "http"
  port: "8090"
  host: "0.0.0.0"
```

Then run the server:

```bash
go run ./cmd/server/main.go
```

## Available MCP Tools

The MCP Server exposes the following tools that can be used by AI models:

### 1. list_data_sources
Lists all configured data sources in Nexus-Gateway.

Parameters: None

Returns: List of data sources with their IDs, names, types, statuses, and connection details.

### 2. execute_sql_query
Executes a SQL query against a specified data source.

Parameters:
- `datasource_id`: The ID of the data source to query (required)
- `sql`: The SQL query to execute (required)
- `batch_size`: Number of records to return per batch (optional, default: 10000)
- `timeout`: Query timeout in seconds (optional, default: 60)

Returns: Query results.

### 3. get_data_source_info
Gets detailed information about a specific data source.

Parameters:
- `datasource_id`: The ID of the data source to get info for (required)

Returns: Detailed information about the data source including configuration and status.

### 4. validate_sql_query
Validates a SQL query without executing it.

Parameters:
- `datasource_id`: The ID of the data source to validate against (required)
- `sql`: The SQL query to validate (required)

Returns: Validation result indicating whether the query is valid.

### 5. list_tables
Lists all tables in a specific data source.

Parameters:
- `datasource_id`: The ID of the data source to list tables from (required)

Returns: List of tables in the specified data source.

## Integration with Claude Desktop

To integrate with Claude Desktop:

1. Install Claude Desktop
2. Enable MCP in Claude Desktop settings
3. Run Nexus-Gateway as an MCP Server with `transport: "stdio"`
4. Claude Desktop will automatically detect and connect to your MCP Server

## Integration with Other MCP Clients

For other MCP-compatible clients, you can run the server with HTTP transport and provide the server URL to the client.

## Development

The MCP Server implementation is located in `internal/mcp/server.go`.

To add new tools:
1. Define the tool parameters
2. Register the tool in `registerTools()` method
3. Implement the handler function
4. Test the new functionality

## Troubleshooting

### Common Issues

1. **MCP client cannot connect**: Ensure the MCP server is running and the transport method matches what the client expects.

2. **Permission errors**: Make sure the MCP server has access to the same data sources as the regular server.

3. **Configuration errors**: Verify that your config.yaml has the correct MCP settings.

## Security Considerations

When running as an MCP Server, be aware that AI models will have access to your data through the exposed tools. Consider:

1. Only expose data sources that are appropriate for AI access
2. Implement proper authentication and authorization
3. Monitor and log all MCP tool usage
4. Review and validate all SQL queries before execution