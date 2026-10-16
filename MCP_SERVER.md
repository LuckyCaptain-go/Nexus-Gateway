# Data Fabric MCP Server

Data Fabric exposes an optional MCP server over stdio. It reuses the configured data sources and query service. HTTP and SSE transports are not implemented.

## Requirements

- Go 1.26.1 or later.
- A configured metadata database.
- Python 3 with sqlglot installed when SQL dialect translation tools are used.
- Start the application from the repository root so the translation script can be located.

## Configure and run

Set the following in configs/config.yaml:

~~~yaml
mcp:
  enabled: true
  transport: "stdio"
~~~

Then run:

~~~bash
go run ./cmd/server
~~~

MCP is disabled by default. When enabled, the process uses stdio and should be launched and managed by a trusted MCP client. Do not redirect application output to the protocol client's stdout.

## Tools

- list_data_sources
- get_data_source_info (does not return usernames or passwords)
- execute_sql_query
- fetch_next_batch (uses the query_id, slug and token returned by execute_sql_query)
- validate_sql_query
- list_tables
- translate_sql
- list_supported_dialects

SQL translation supports MySQL, PostgreSQL, Trino, Spark, Oracle, SQL Server, Snowflake, SQLite, DuckDB and Redshift, with aliases for some compatible database types. Successful translation does not guarantee semantic equivalence on the target database.

## Security

The MCP process runs with the permissions of its parent client. Any connected client can invoke the listed tools against the configured data sources. Configure it only for trusted clients and use least-privilege database accounts. This implementation has no separate MCP user authentication or per-user authorization, and it does not expose an HTTP/SSE endpoint.
