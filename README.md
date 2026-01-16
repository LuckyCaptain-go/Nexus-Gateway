# Nexus-Gateway

<div align="center">

**Universal Multi-Database Gateway for Unified Data Access**

[![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8?style=flat&logo=go)](https://golang.org/)
[![Gin](https://img.shields.io/badge/Gin-Web-Framework-green?style=flat)](https://gin-gonic.com/)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue?style=flat)](LICENSE)
[![Drivers](https://img.shields.io/badge/Database_Drivers-90+-brightgreen?style=flat)]()

</div>

## Overview

Nexus-Gateway is a universal multi-database proxy gateway written in Go that provides **unified SQL access to 90+ data sources** across cloud warehouses, data lakes, OLAP engines, object storage, distributed file systems, and domestic databases. It acts as a secure query proxy that routes SQL queries to the appropriate data source based on UUID-based identification.

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

## API Usage

For detailed API usage examples, see [API Usage Guide](docs/api_usage.md).

Basic query example:
```bash
curl -X POST http://localhost:8099/api/v1/query \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -d '{
    "dataSourceId": "uuid-of-datasource",
    "sql": "SELECT * FROM table WHERE condition = ?",
    "parameters": ["value"],
    "limit": 100
  }'
```

## Architecture

Nexus-Gateway follows a clean, modular architecture:

```
Nexus-Gateway/
├── cmd/server/                    # Application entry point
├── internal/
│   ├── controller/               # HTTP request handlers
│   ├── service/                  # Business logic layer
│   ├── repository/               # Data access layer
│   ├── model/                    # Data models and DTOs
│   ├── middleware/               # HTTP middleware (JWT, rate limiting, etc.)
│   └── database/
│       └── drivers/              # Database drivers (90+ implementations)
│           ├── table_format/     # Iceberg, Delta Lake, Hudi
│           ├── warehouses/       # Snowflake, Databricks, Redshift, BigQuery
│           ├── olap/             # ClickHouse, Doris, StarRocks, Druid
│           ├── object_storage/   # S3, MinIO, OSS, COS, Azure Blob
│           ├── file_system/      # HDFS, Apache Ozone
│           └── domestic/         # OceanBase, TiDB, TDSQL, etc.
├── configs/                      # Configuration files
├── pkg/                          # Reusable packages
├── docs/                         # Detailed documentation
└── scripts/                      # Build and deployment scripts
```

For detailed architecture information, see [Architecture Guide](docs/architecture.md).

## Configuration

See [Configuration Guide](docs/configuration.md) for detailed configuration options.

## Development

### Adding a New Database Driver

1. Create a new file in `internal/database/drivers/<category>/`
2. Implement the `Driver` interface:

```go
type Driver interface {
    Open(dsn string) (*sql.DB, error)
    ValidateDSN(dsn string) error
    GetDefaultPort() int
    BuildDSN(config *model.DataSourceConfig) string
    GetDatabaseTypeName() string
    TestConnection(db *sql.DB) error
    GetDriverName() string
    GetCategory() DriverCategory
    GetCapabilities() DriverCapabilities
    ConfigureAuth(authConfig interface{}) error
}
```

3. Register the driver in the driver registry

For detailed development information, see [Development Guide](docs/development.md).

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

## Contributing

We welcome contributions! Please see our contributing guidelines:

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Write tests for new functionality
4. Ensure all tests pass (`go test ./...`)
5. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
6. Push to the branch (`git push origin feature/AmazingFeature`)
7. Open a Pull Request

For more details, see [Contributing Guide](docs/contributing.md).

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Support

- Documentation: [Full Docs](https://LuckyCaptain-go.github.io/Nexus-Gateway)
- Issues: [GitHub Issues](https://github.com/LuckyCaptain-go/Nexus-Gateway/issues)
- Discussions: [GitHub Discussions](https://github.com/LuckyCaptain-go/Nexus-Gateway/discussions)

## Acknowledgments

- Built with [Gin](https://gin-gonic.com/) web framework
- ORM powered by [GORM](https://gorm.io/)
- Database drivers inspired by various open-source projects
- Thanks to all contributors who helped implement 90+ database drivers

---

<div align="center">
Made with ❤️ by the Nexus-Gateway team
</div>
