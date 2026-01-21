@echo off
REM Script to run Nexus-Gateway as MCP Server

echo Starting Nexus-Gateway as MCP Server...

REM Set MCP configuration via environment variables
set CONFIG_FILE=configs/config.yaml

REM Run the server
go run ./cmd/server/main.go

pause