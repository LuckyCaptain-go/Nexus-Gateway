# Multi-Database Proxy Gateway Implementation Plan (Go)

## 1. Technology Stack

### Backend Framework
- **Language**: Go 1.21+ (Latest stable)
- **Web Framework**: Gin - High performance HTTP framework
- **Alternative**: Echo or standard library net/http
- **Chosen**: Gin for excellent performance and middleware support

### Database Connectivity
- **ORM**: GORM - Feature-rich Go ORM library
- **Database Drivers**:
  - MySQL: go-sql-driver/mysql
  - PostgreSQL: lib/pq
  - Oracle: go-ora
- **Connection Pooling**: Built-in sql.DB with custom pool management

### Security & Validation
- **SQL Injection Prevention**: GORM with parameterized queries
- **SQL Parser**: vitess.io/vitess/sqlparser for SQL statement validation
- **Authentication**: JWT tokens with golang-jwt/jwt
- **Input Validation**: go-playground/validator

### Configuration & Storage
- **Data Source Storage**: MySQL with GORM
- **Configuration**: Viper for configuration management
- **Secret Management**: Environment variables or HashiCorp Vault

### API & Documentation
- **API Framework**: Gin
- **OpenAPI**: swaggo/gin-swagger for auto documentation
- **Response Format**: JSON with struct tags

### Observability
- **Logging**: Logrus or zap with structured logging
- **Metrics**: Prometheus client for Go
- **Tracing**: OpenTelemetry Go SDK
- **Correlation IDs**: Custom middleware

## 2. Project Structure

```
multi-database-proxy-gateway/
├── cmd/
│   └── server/
│       └── main.go              # Application entry point
├── internal/
│   ├── config/                  # Configuration
│   │   ├── config.go
│   │   └── database.go
│   ├── controller/              # HTTP handlers
│   │   ├── query_controller.go
│   │   ├── datasource_controller.go
│   │   └── health_controller.go
│   ├── service/                 # Business logic
│   │   ├── query_service.go
│   │   ├── datasource_service.go
│   │   └── connection_service.go
│   ├── repository/              # Data access layer
│   │   ├── datasource_repository.go
│   │   └── interfaces.go
│   ├── model/                   # Data models
│   │   ├── datasource.go
│   │   ├── query.go
│   │   └── response.go
│   ├── security/                # Security components
│   │   ├── sql_validator.go
│   │   ├── auth_middleware.go
│   │   └── jwt.go
│   ├── middleware/              # HTTP middleware
│   │   ├── cors.go
│   │   ├── logging.go
│   │   ├── metrics.go
│   │   └── correlation.go
│   ├── database/                # Database connections
│   │   ├── connection_pool.go
│   │   ├── drivers.go
│   │   └── health_checker.go
│   ├── utils/                   # Utilities
│   │   ├── data_type_mapper.go
│   │   ├── uuid.go
│   │   ├── encryption.go
│   │   └── error.go
│   └── exception/               # Exception handling
│       └── handlers.go
├── pkg/                         # Public libraries
│   └── response/
│       └── standardized.go
├── docs/                        # API documentation
│   └── swagger.yaml
├── configs/                     # Configuration files
│   ├── config.yaml
│   └── config.dev.yaml
├── migrations/                  # Database migrations
│   └── 001_create_datasources.sql
├── tests/                       # Test files
│   ├── integration/
│   ├── unit/
│   └── fixtures/
├── scripts/                     # Build and deployment scripts
│   ├── build.sh
│   └── docker.sh
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
├── go.mod
├── go.sum
├── Makefile
└── README.md
```

## 3. Key Dependencies

### Core Dependencies (go.mod)
```go
module nexus-gateway

go 1.21

require (
    // Web Framework
    github.com/gin-gonic/gin v1.9.1

    // Database
    gorm.io/gorm v1.25.4
    gorm.io/driver/mysql v1.5.1
    gorm.io/driver/postgres v1.5.2
    github.com/sijms/go-ora/v2 v2.7.4

    // Configuration
    github.com/spf13/viper v1.16.0

    // Authentication
    github.com/golang-jwt/jwt/v5 v5.0.0

    // Validation
    github.com/go-playground/validator/v10 v10.15.1

    // SQL Parsing
    vitess.io/vitess/go/vt/sqlparser v0.17.0

    // Logging
    github.com/sirupsen/logrus v1.9.3

    // Metrics & Tracing
    github.com/prometheus/client_golang v1.16.0
    go.opentelemetry.io/otel v1.19.0
    go.opentelemetry.io/otel/trace v1.19.0

    // Documentation
    github.com/swaggo/gin-swagger v1.6.0
    github.com/swaggo/files v1.0.1
    github.com/swaggo/swag v1.16.1

    // UUID
    github.com/google/uuid v1.3.0

    // Encryption
    golang.org/x/crypto v0.13.0
)
```

### Development Dependencies
```go
require (
    // Testing
    github.com/stretchr/testify v1.8.4
    github.com/golang/mock v1.6.0
    github.com/DATA-DOG/go-sqlmock v1.5.0

    // Testcontainers
    github.com/testcontainers/testcontainers-go v0.24.0
    github.com/testcontainers/testcontainers-go/modules/mysql v0.24.0
)
```

## 4. Database Schema

### Data Sources Table (MySQL)
```sql
CREATE TABLE data_sources (
    id BINARY(16) PRIMARY KEY COMMENT 'UUID stored as binary',
    name VARCHAR(255) NOT NULL COMMENT 'Human readable name',
    type ENUM('mysql', 'mariadb', 'postgresql', 'oracle') NOT NULL,
    config JSON NOT NULL COMMENT 'Connection configuration',
    status ENUM('active', 'inactive', 'error') DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    INDEX idx_status (status),
    INDEX idx_type (type),
    UNIQUE KEY uk_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Data source configurations';
```

### Sample Configuration JSON
```json
{
  "host": "db.example.com",
  "port": 3306,
  "database": "production",
  "username": "readonly_user",
  "password": "AES256:encrypted_base64_password",
  "ssl": true,
  "connectionTimeout": 30000,
  "maxPoolSize": 10,
  "idleTimeout": 600000,
  "maxLifetime": 1800000,
  "timezone": "UTC"
}
```

## 5. Core Components Design

### 5.1 Connection Pool Manager
```go
type ConnectionPool struct {
    pools map[string]*sql.DB
    mutex sync.RWMutex
    config *Config
}

func (cp *ConnectionPool) GetConnection(uuid string) (*sql.DB, error)
func (cp *ConnectionPool) CreatePool(config DataSourceConfig) error
func (cp *ConnectionPool) HealthCheck() map[string]bool
```

### 5.2 SQL Validator
```go
type SQLValidator struct{}

func (sv *SQLValidator) ValidateStatement(sql string) error
func (sv *SQLValidator) IsReadOnly(sql string) (bool, error)
func (sv *SQLValidator) ParseSQL(sql string) (ast.Statement, error)
```

### 5.3 Query Service
```go
type QueryService struct {
    poolMgr *ConnectionPool
    dsRepo  DataSourceRepository
    validator SQLValidator
}

func (qs *QueryService) ExecuteQuery(req QueryRequest) (*QueryResponse, error)
func (qs *QueryService) StandardizeResult(rows *sql.Rows, dbType string) (*StandardizedResult, error)
```

## 6. API Design

### Query Endpoint
```
POST /api/v1/query
Content-Type: application/json
Authorization: Bearer <jwt_token>

{
  "dataSourceId": "550e8400-e29b-41d4-a716-446655440000",
  "sql": "SELECT id, name FROM users WHERE status = ? AND created_at > ?",
  "parameters": ["active", "2023-01-01"],
  "limit": 1000,
  "offset": 0
}
```

### Response Format
```json
{
  "success": true,
  "data": {
    "columns": [
      {"name": "id", "type": "integer", "nullable": false},
      {"name": "name", "type": "string", "nullable": false}
    ],
    "rows": [
      [1, "John Doe"],
      [2, "Jane Smith"]
    ],
    "metadata": {
      "rowCount": 2,
      "executionTimeMs": 45,
      "dataSourceId": "550e8400-e29b-41d4-a716-446655440000",
      "databaseType": "mysql",
      "hasMore": false
    }
  },
  "correlationId": "req_123456789",
  "timestamp": "2025-12-22T10:30:00Z"
}
```

## 7. Implementation Strategy

### Phase 1: Foundation (MVP)
- Basic project structure with Gin framework
- MySQL connectivity and query execution
- Simple data source management (MySQL storage)
- Basic error handling

### Phase 2: Multi-Database Support
- PostgreSQL and Oracle driver integration
- Connection pool management
- Result set standardization
- Comprehensive error handling

### Phase 3: Security & Validation
- SQL statement validation
- JWT authentication
- Input validation and sanitization
- Rate limiting

### Phase 4: Observability & Monitoring
- Structured logging with correlation IDs
- Prometheus metrics
- Health check endpoints
- Performance optimization

### Phase 5: Production Readiness
- Configuration management
- Docker containerization
- CI/CD pipeline
- Load testing and optimization

### Phase 6: Enhanced Single-Source Capabilities
- **Data Lake & Warehouse Support**:
  - Apache Iceberg, Delta Lake, Apache Hudi table formats
  - Snowflake, Databricks, Redshift, BigQuery connectors
  - ClickHouse, Doris, StarRocks, Druid drivers
- **Object Storage Integration**:
  - S3, MinIO, OSS, COS, Azure Blob clients
  - HDFS, Apache Ozone support
  - Parquet, ORC, Avro, CSV, JSON file readers
- **Domestic Database Support**:
  - OceanBase, TiDB, TDSQL, GaussDB drivers
  - DaMeng, KingbaseES, GBase, Oscar, OpenGauss support

### Phase 7: Compute Engine Integration
- **Trino Integration**: Query federation via Trino JDBC/REST API
- **Spark Integration**: Spark Connect, Livy REST API integration
- **Flink Integration**: SQL Gateway, Table API support
- **Engine Router**: Intelligent routing based on query type and data size

### Phase 8: Cross-Source Query via Compute Engines
- **Unified Query API**: Single endpoint that detects multi-source queries and routes to Trino/Spark
- **Source Catalog Management**: Automatically register data sources as Trino/Spark catalogs
- **Query Translation Layer**: Handle SQL dialect differences between engines
- **Result Streaming Proxy**: Stream large result sets efficiently from compute engines
- **Engine Integration**: Trino JDBC/REST, Spark Connect, Flink SQL Gateway

### Phase 9: ETL via Compute Engines
- **ETL Job Submission API**: REST endpoints to submit Spark/Flink jobs
- **Job Lifecycle Management**: Track job submission, execution, completion, failure
- **Pre-built Templates**: Common ETL patterns (CDC, batch sync, aggregation)
- **Scheduler Integration**: Interfaces for Airflow, DolphinScheduler, Cron
- **Pipeline Designer UI**: Visual builder that generates Spark/Flink job definitions

## 8. Development Guidelines

### Code Organization
- Follow Clean Architecture principles
- Separate concerns (handler, service, repository)
- Use dependency injection
- Implement interfaces for testing

### Error Handling
- Use custom error types
- Provide structured error responses
- Log errors with correlation IDs
- Graceful degradation

### Performance Considerations
- Connection pooling and reuse
- Prepared statements when possible
- Result set streaming for large datasets
- Memory optimization

### Testing Strategy
- Unit tests: 80%+ coverage
- Integration tests with testcontainers
- API contract tests
- Performance benchmarking