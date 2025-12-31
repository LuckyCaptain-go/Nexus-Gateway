# Nexus-Gateway

<div align="center">

**Universal Multi-Database Gateway for Unified Data Access**

[![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8?style=flat&logo=go)](https://golang.org/)
[![Gin](https://img.shields.io/badge/Gin-Web-Framework-green?style=flat)](https://gin-gonic.com/)
[![License](https://img.shields.io/badge/license-MIT-blue?style=flat)](LICENSE)
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

## Supported Data Sources

### Cloud Data Warehouses (4 drivers)
| Database | Features |
|----------|----------|
| **Snowflake** | Warehouse management, time travel, schema discovery, type mapping (VARIANT/ARRAY/OBJECT) |
| **Databricks** | Delta Lake integration, SQL warehouse REST API, statement polling, async execution |
| **Redshift** | IAM authentication, PostgreSQL-compatible, Redshift Spectrum support |
| **BigQuery** | Native Go client, pagination, STRUCT/ARRAY type mapping, query jobs API |

### Data Lake Table Formats (3 drivers)
| Format | Features |
|--------|----------|
| **Apache Iceberg** | Snapshot-based time travel, schema evolution, partition discovery |
| **Delta Lake** | Time travel, transaction log (ACID), versioning, vacuum operations |
| **Apache Hudi** | COPY_ON_WRITE / MERGE_ON_READ, instant time travel, timeline queries |

### OLAP Engines (4 drivers)
| Engine | Features |
|--------|----------|
| **ClickHouse** | Native protocol, materialized views, dictionaries, array types, compression |
| **Apache Doris** | Rollup tables, materialized views, pipeline engine, optimization hints |
| **StarRocks** | Pipeline engine execution, table distribution, parallel execution |
| **Apache Druid** | SQL API via REST, timeseries queries, TopN, GroupBy, aggregations |

### Object Storage (25+ drivers)
| Storage | File Formats Supported |
|---------|----------------------|
| **AWS S3** | Parquet, ORC, Avro, CSV, JSON, Iceberg, Delta Lake, Hudi (with S3 Select API) |
| **MinIO** | Parquet, ORC, CSV, JSON, Delta Lake, Iceberg (S3-compatible) |
| **Alibaba OSS** | Parquet, CSV, JSON, Delta Lake |
| **Tencent COS** | Parquet, CSV, JSON, Delta Lake |
| **Azure Blob** | Parquet, CSV, JSON, Delta Lake, SAS token support |

### File Systems (24+ drivers)
| System | File Formats Supported |
|--------|----------------------|
| **HDFS** | Parquet, ORC, Avro, CSV, JSON, XML, Text, Iceberg, Delta Lake, Hudi (with compression) |
| **Apache Ozone** | Parquet, ORC, Avro, CSV, JSON, XML, Text, Iceberg, Delta Lake, Hudi |

### Domestic Chinese Databases (9 drivers)
| Database | Features |
|----------|----------|
| **OceanBase** | MySQL & Oracle compatibility modes, automatic mode detection |
| **TiDB** | Distributed query optimization, placement rules, hot region detection |
| **TDSQL** | Sharding management, read-write splitting, consistency levels |
| **GaussDB** | High availability (HA) setup, streaming replication, failover support |
| **DaMeng (DM)** | Charset support (UTF8, GB18030), flashback queries, protocol features |
| **KingbaseES** | Oracle compatibility mode, sequences, synonyms, packages, database links |
| **GBase 8s** | Informix compatibility, charset conversion, fragment management |
| **Oscar (ShenTong)** | Cluster mode support, partition distribution, fragment management |
| **OpenGauss** | Row-level security (RLS), distributed transactions, table distribution |

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
├── docs/                         # API documentation
├── deployments/                  # Docker and K8s configs
└── scripts/                      # Build and deployment scripts
```

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Language | Go 1.25+ |
| Web Framework | Gin |
| ORM | GORM |
| Database Drivers | Custom driver implementations (90+) |
| Security | JWT, SQL Validation, Rate Limiting |
| Configuration | Viper |
| Documentation | Swagger/OpenAPI |
| Containerization | Docker, Kubernetes |

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

## API Usage Examples

### Querying Snowflake Data Warehouse

```bash
curl -X POST http://localhost:8099/api/v1/query \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -d '{
    "dataSourceId": "uuid-of-snowflake-datasource",
    "sql": "SELECT * FROM orders WHERE order_date >= ?",
    "parameters": ["2024-01-01"],
    "limit": 100
  }'
```

### Querying S3 Parquet Files

```bash
curl -X POST http://localhost:8099/api/v1/query \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -d '{
    "dataSourceId": "uuid-of-s3-parquet-datasource",
    "sql": "SELECT * FROM s3://my-bucket/data/*.parquet WHERE year = ? AND month = ?",
    "parameters": ["2024", "01"],
    "limit": 1000
  }'
```

### Time Travel Query (Delta Lake/Iceberg/Hudi)

```bash
curl -X POST http://localhost:8099/api/v1/query \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -d '{
    "dataSourceId": "uuid-of-delta-lake-datasource",
    "sql": "SELECT * FROM orders VERSION AS OF 5",  -- Delta Lake
    "limit": 100
  }'
```

### Querying TiDB with Distribution Info

```bash
curl -X POST http://localhost:8099/api/v1/query \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -d '{
    "dataSourceId": "uuid-of-tidb-datasource",
    "sql": "SELECT * FROM users WHERE id = ?",
    "parameters": ["12345"],
    "explain": true  -- Returns region distribution info
  }'
```

### Streaming Large Result Sets (new)

For large result sets the gateway supports a streaming mode that keeps a server-side cursor
open and returns results in batches to avoid re-executing the full query with LIMIT/OFFSET.

- Use `/api/v1/query` for an inline query. When the backend supports streaming, the
  response may contain a `nextUri` field. Call that URI to retrieve the next batch.
- Use `/api/v1/fetch` to explicitly request batch/streaming behavior. This endpoint
  attempts to open a streaming cursor and returns a `queryId`, `slug`, `token`, and
  `nextUri` when more data is available.

Example: Execute query and get `nextUri` from `/query` (or use `/fetch`):

```bash
curl -X POST http://localhost:8099/api/v1/query \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -d '{
    "dataSourceId": "uuid-of-large-datasource",
    "sql": "SELECT * FROM very_large_table WHERE created_at >= ?",
    "parameters": ["2024-01-01"],
    "limit": 1000
  }'
```

If the response contains `nextUri`, call it to fetch subsequent batches:

```bash
curl -X GET "http://localhost:8099/api/v1/fetch/{queryId}/{slug}/{token}?batch_size=1000" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN"
```

Notes:

- The gateway will try to establish a streaming cursor (server-side rows). If the
  backend does not support streaming and `query.prefer_streaming` is `false`, the
  gateway automatically falls back to LIMIT/OFFSET continuation.
- Streaming sessions are maintained server-side for a configurable TTL (default 30 minutes).
- Always close or exhaust the stream; the gateway will clean up expired sessions automatically.


## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/health` | Health check |
| GET | `/api/v1/drivers` | List all supported database drivers |
| GET | `/api/v1/datasources` | List all data sources |
| POST | `/api/v1/datasources` | Create data source |
| GET | `/api/v1/datasources/:id` | Get data source details |
| PUT | `/api/v1/datasources/:id` | Update data source |
| DELETE | `/api/v1/datasources/:id` | Delete data source |
| POST | `/api/v1/query` | Execute SQL query |
| POST | `/api/v1/query/validate` | Validate SQL query |
| POST | `/api/v1/datasources/:id/test` | Test database connection |
| GET | `/api/v1/datasources/:id/schema` | Get schema information |

## Configuration

### Example Configuration (`configs/config.yaml`)

```yaml
server:
  port: 8099
  mode: release  # debug, release, production
  host: "0.0.0.0"

database:
  host: "localhost"
  port: 3306
  username: "nexus"
  password: "your_password"
  database: "nexus_gateway"

security:
  jwt:
    secret: "your-jwt-secret"
    expiration: 24h
  rateLimit:
    enabled: true
    requests: 100
    window: 1m

logging:
  level: info
  format: json

drivers:
  # Cloud warehouse credentials
  snowflake:
    account: "your-account"
    warehouse: "COMPUTE_WH"
  databricks:
    workspace_url: "https://your-workspace.cloud.databricks.com"
    http_path: "/sql/protocolv1/o/0/abcd-1234"

  # Object storage credentials
  aws:
    region: "us-west-2"
    access_key_id: "YOUR_ACCESS_KEY"
    secret_access_key: "YOUR_SECRET_KEY"
  azure:
    account_name: "yourstorageaccount"
    account_key: "your-account-key"

  # Domestic database credentials
  oceanbase:
    mode: "mysql"  # or "oracle"
  tidb:
    pd_addresses: ["pd1:2379", "pd2:2379"]

query:
  # When true, the gateway prefers establishing a server-side streaming cursor
  # for large result sets. If the backend driver doesn't support streaming and
  # prefer_streaming is true, the query will fail. If set to false, the gateway
  # will fall back to LIMIT/OFFSET continuation automatically when streaming
  # is not available.
  prefer_streaming: true
```

## Security Features

### SQL Injection Prevention
- Only SELECT statements allowed by default
- Parameterized queries for all database types
- Query validation before execution
- Support for prepared statements

### Authentication & Authorization
- JWT-based authentication
- Token-based API access
- Role-based access control (RBAC) ready
- Configurable token expiration

### Rate Limiting
- Per-IP and per-user rate limits
- Configurable time windows
- Sliding window algorithm

### Audit & Monitoring
- Correlation ID for request tracking
- Comprehensive query execution logs
- Performance metrics (execution time, rows processed)
- Error tracking and alerting

## Driver Implementation Statistics

| Category | Driver Count | Total Files |
|----------|--------------|-------------|
| Table Formats | 3 | 12 files |
| Cloud Warehouses | 4 | 11 files |
| OLAP Engines | 4 | 17 files |
| Object Storage | 5 providers | 31 files |
| File Systems | 2 systems | 24 files |
| Domestic Databases | 9 databases | 27 files |
| **Total** | **27 types** | **122+ files** |

## Advanced Features

### Schema Discovery
Automatic schema detection for:
- Parquet/ORC/Avro file schemas
- CSV delimiter and type inference
- JSON nested structure parsing
- Database table metadata retrieval

### Time Travel Queries
Support for historical data queries:
- **Delta Lake**: `VERSION AS OF`, `TIMESTAMP AS OF`
- **Apache Iceberg**: Snapshot-based queries
- **Apache Hudi**: Point-in-time queries
- **Snowflake**: Time travel support
- **BigQuery**: `FOR SYSTEM_TIME AS OF`

### Distributed Query Features
- **TiDB**: Placement rules, region distribution, hot spot detection
- **OceanBase**: Compatibility mode detection, MySQL/Oracle modes
- **GaussDB**: HA setup, streaming replication, failover
- **TDSQL**: Sharding info, read-write splitting

### Query Optimization
- Query hints for OLAP engines
- Materialized view support
- Rollup table optimization
- Pipeline engine execution
- Predicate pushdown for Parquet/ORC

## Development

### Project Structure

```
Nexus-Gateway/
├── cmd/
│   └── server/
│       └── main.go              # Application entry point
├── internal/
│   ├── controller/              # HTTP request handlers
│   │   ├── query_controller.go
│   │   ├── datasource_controller.go
│   │   └── health_controller.go
│   ├── service/                 # Business logic layer
│   │   ├── query_service.go
│   │   ├── datasource_service.go
│   │   └── driver_service.go
│   ├── repository/              # Data access layer
│   │   ├── datasource_repository.go
│   │   └── query_history_repository.go
│   ├── model/                   # Data models
│   │   ├── datasource.go
│   │   ├── query.go
│   │   └── result.go
│   ├── middleware/              # Middleware
│   │   ├── auth.go
│   │   ├── rate_limit.go
│   │   └── logger.go
│   └── database/
│       └── drivers/             # Database drivers
│           ├── driver.go        # Driver interface
│           ├── registry.go      # Driver registry
│           ├── table_format/    # Iceberg, Delta, Hudi
│           ├── warehouses/      # Snowflake, Databricks, etc.
│           ├── olap/            # ClickHouse, Doris, etc.
│           ├── object_storage/  # S3, MinIO, etc.
│           ├── file_system/     # HDFS, Ozone, etc.
│           └── domestic/        # OceanBase, TiDB, etc.
├── configs/                     # Configuration files
├── docs/                        # API documentation
├── deployments/                 # Docker & K8s configs
└── scripts/                     # Build & deployment scripts
```

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
4. Add configuration support
5. Update documentation

## Production Deployment

### Environment Variables

```bash
export SERVER_MODE=production
export DATABASE_HOST=your-db-host
export DATABASE_PASSWORD=secure-password
export JWT_SECRET=your-jwt-secret
```

### Docker Compose

```bash
docker-compose up -d
```

### Kubernetes

```bash
kubectl apply -f deployments/k8s/
```

## Performance Tuning

- **Connection Pooling**: Configure `maxOpenConns` and `maxIdleConns` per datasource
- **Query Timeout**: Set appropriate timeout values
- **Streaming Results**: Enable streaming for large result sets
- **Caching**: Use materialized views in OLAP engines
- **Predicate Pushdown**: Leverage Parquet/ORC statistics
- **Parallel Execution**: Configure parallel instance counts for distributed queries

## Monitoring & Observability

Built-in metrics and monitoring:
- Query execution statistics
- Database connection pool metrics
- API response times
- Error rates and types
- Driver-specific metrics (e.g., ClickHouse dictionaries, Doris rollups)
- Health check endpoints

Integrate with Prometheus, Grafana, or other monitoring tools via the `/metrics` endpoint.

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

### 📋 Phase 3: Cross-Source Query via Compute Engines
- [ ] **Unified Query API**: Single interface routing to Trino/Spark
- [ ] **Source Catalog Management**: Map data sources to Trino/Spark catalogs
- [ ] **Query Translation**: Convert SQL to engine-specific syntax
- [ ] **Result Proxy**: Stream results from compute engines
- [ ] **Engine Health Check**: Monitor cluster status

### 📋 Phase 4: ETL via Compute Engines
- [ ] **ETL Job API**: Submit Spark/Flink ETL jobs
- [ ] **Job Management**: Track status, logs, cancellation
- [ ] **Template Library**: Pre-built ETL templates
- [ ] **Schedule Integration**: Airflow, DolphinScheduler
- [ ] **Visual Pipeline Builder**: Web UI for pipeline design

### 📋 Phase 5: Advanced Features
- [ ] Query result caching with intelligent invalidation
- [ ] GraphQL API support
- [ ] Webhook notifications
- [ ] Advanced analytics dashboard
- [ ] Multi-region deployment
- [ ] Query auditing and compliance
- [ ] AI-powered query optimization

## Contributing

We welcome contributions! Please see our contributing guidelines:

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Write tests for new functionality
4. Ensure all tests pass (`go test ./...`)
5. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
6. Push to the branch (`git push origin feature/AmazingFeature`)
7. Open a Pull Request

### Adding New Drivers

We especially welcome contributions for new database drivers! See the "Adding a New Database Driver" section above.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

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
