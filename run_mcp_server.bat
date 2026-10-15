@echo off
REM Start Data Fabric in MCP stdio mode.
echo Starting Data Fabric MCP server...
echo Set mcp.enabled to true and mcp.transport to stdio in configs\config.yaml first.

go run ./cmd/server
if errorlevel 1 pause
