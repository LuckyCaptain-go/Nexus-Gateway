# Nexus-Gateway

<div align="center">

**Universal Multi-Database Gateway for Unified Data Access**

[![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8?style=flat&logo=go)](https://golang.org/)
[![Gin](https://img.shields.io/badge/Gin-Web-Framework-green?style=flat)](https://gin-gonic.com/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue?style=flat)](LICENSE)
[![Drivers](https://img.shields.io/badge/Database_Drivers-90+-brightgreen?style=flat)]()

</div>

## 🌐 Language Versions / 语言版本

- [English Version](README_en.md)
- [中文版本](README_zh.md)

## Overview

**Nexus-Gateway** is an enterprise-grade universal data gateway that serves as the foundational data access layer for AI agents, RAG systems, BI reports, and data scientists. It provides secure, unified SQL access to 90+ data sources with smart routing and extensible architecture for diverse analytical needs.

## Key Features

- **🚀 Universal Data Access** - Support for 90+ database drivers including:
  - **Cloud Data Warehouses**: Snowflake, Databricks, Redshift, BigQuery
  - **Data Lake Tables**: Apache Iceberg, Delta Lake, Apache Hudi
  - **OLAP Engines**: ClickHouse, Apache Doris, StarRocks, Apache Druid
  - **Object Storage**: AWS S3, MinIO, Alibaba OSS, Tencent COS, Azure Blob Storage
  - **File Systems**: HDFS, Apache Ozone
  - **File Formats**: Parquet, ORC, Avro, CSV, JSON, XML, Text
  - **Domestic Databases**: OceanBase, TiDB, TDSQL, GaussDB, DaMeng, KingbaseES, GBase, Oscar, OpenGauss
- **🔒 Security First** - SQL injection prevention, JWT authentication, rate limiting, read-only query enforcement
- **⚡ High Performance** - Connection pooling, query optimization, streaming support for large datasets
- **🌊 Time Travel Support** - Query historical data in Iceberg, Delta Lake, and Hudi
- **📊 Schema Discovery** - Automatic schema detection for file formats and databases
- **🐳 Docker & Kubernetes Ready** - Multi-stage Dockerfile and K8s manifests included
- **📖 Comprehensive API** - RESTful API with Swagger documentation
- **🔧 Extensible Architecture** - Easy to add new database drivers via plugin system
- **🤖 AI Integration Ready** - **NEW!** Supports Model Context Protocol (MCP) for AI agent integration

## Quick Start

### Prerequisites

- Go 1.25 or higher
- Docker (optional)

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/LuckyCaptain-go/Nexus-Gateway.git
   cd Nexus-Gateway
   ```

2. **Install dependencies**
   ```bash
   go mod download
   ```

3. **Configure the application**
   ```bash
   cp configs/config.yaml.example configs/config.yaml
   # Edit configs/config.yaml with your settings
   ```

4. **Run the application**
   ```bash
   go run cmd/server/main.go
   ```

The server will start on `http://localhost:8099`

### MCP Server Mode (AI Integration)

**NEW!** Run Nexus-Gateway as an MCP Server to connect AI models directly to your data sources:

1. **Configure MCP in config.yaml**
   ```yaml
   mcp:
     enabled: true
     transport: "stdio"  # or "http"
     port: "8090"
     host: "0.0.0.0"
   ```

2. **Start in MCP mode**
   ```bash
   go run cmd/server/main.go
   ```

3. **Connect to AI clients** such as Claude Desktop, VSCode, Cursor, etc.

### Docker Deployment

```bash
# Build the image
docker build -t nexus-gateway:latest .

# Run the container
docker run -d \
  --name nexus-gateway \
  -p 8099:8099 \
  -v $(pwd)/configs:/app/configs \
  nexus-gateway:latest
```

## AI Integration - MCP Server

Nexus-Gateway now supports the Model Context Protocol (MCP), allowing AI agents to directly access your data sources through standardized tools:

- **list_data_sources**: Discover available data sources
- **execute_sql_query**: Execute SQL queries on connected databases
- **get_data_source_info**: Retrieve detailed information about data sources
- **validate_sql_query**: Validate SQL queries before execution
- **list_tables**: List all tables in a specific data source

This enables RAG (Retrieval Augmented Generation) systems and AI agents to access your structured data seamlessly.

## Roadmap

### ✅ Phase 1: Enhanced Single-Source Capabilities (COMPLETED)

#### Data Lakes & Warehouses ✅
- [x] **Table Formats**: Apache Iceberg, Delta Lake, Apache Hudi
- [x] **Cloud Data Warehouses**: Snowflake, Databricks, Redshift, BigQuery
- [x] **OLAP Engines**: ClickHouse, Apache Doris, StarRocks, Apache Druid

#### Object Storage & File Systems ✅
- [x] **Object Storage**: AWS S3, MinIO, Alibaba OSS, Tencent COS, Azure Blob
- [x] **Distributed Storage**: HDFS, Apache Ozone
- [x] **File Formats**: Parquet, ORC, Avro, CSV, JSON, XML, Text (with compression)

#### Domestic Database Support (China) ✅
- [x] **Distributed Databases**: OceanBase, TiDB, Tencent TDSQL, GaussDB
- [x] **Traditional Databases**: DaMeng (DM), KingbaseES, GBase, Oscar, OpenGauss

### 🔄 Phase 2: Compute Engine Integration (IN PROGRESS)
- [ ] **Trino Integration**: Distributed SQL query engine federation
- [ ] **Spark Integration**: Batch and streaming data processing
- [ ] **Flink Integration**: Real-time stream processing
- [ ] **Compute Engine Orchestration**: Intelligent routing to optimal engine

### 📋 Phase 3-5: See [Roadmap Details](docs/roadmap.md)

## Support

- Documentation: [Full Docs](https://LuckyCaptain-go.github.io/Nexus-Gateway)
- Issues: [GitHub Issues](https://github.com/LuckyCaptain-go/Nexus-Gateway/issues)
- Discussions: [GitHub Discussions](https://github.com/LuckyCaptain-go/Nexus-Gateway/discussions)
- MCP Documentation: [MCP Guide](MCP_DOCUMENTATION.md)

---

<div align="center">
Made with ❤️ by the Nexus-Gateway team
</div>