# Nexus-Gateway

<div align="center">

**A Multi-Database Proxy Gateway for Secure, Unified SQL Query Execution**

[![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8?style=flat&logo=go)](https://golang.org/)
[![Gin](https://img.shields.io/badge/Gin-Web-Framework-green?style=flat)](https://gin-gonic.com/)
[![License](https://img.shields.io/badge/license-MIT-blue?style=flat)](LICENSE)

</div>

## Overview

Nexus-Gateway is a powerful multi-database proxy gateway written in Go that provides a unified interface to execute SQL queries against multiple relational database types. It acts as a secure query proxy that routes read-only SQL queries to the appropriate database based on UUID-based data source identification.

### Key Features

- **Multi-Database Support** - MySQL, MariaDB, PostgreSQL, and Oracle
- **Security First** - SQL injection prevention, JWT authentication, rate limiting
- **Connection Pooling** - Efficient database connection management
- **Query Validation** - Read-only query enforcement (SELECT statements only)
- **Health Monitoring** - Database connectivity checks and statistics
- **High Performance** - Built with Go and Gin framework
- **Docker Support** - Multi-stage Dockerfile for easy deployment
- **RESTful API** - Clean and intuitive API design

## Architecture

Nexus-Gateway follows a clean architecture pattern with the following layers:

```
Nexus-Gateway/
├── cmd/server/          # Application entry point
├── internal/
│   ├── controller/      # HTTP request handlers
│   ├── service/         # Business logic layer
│   ├── repository/      # Data access layer
│   ├── model/           # Data models and DTOs
│   ├── middleware/      # HTTP middleware
│   └── infrastructure/  # Infrastructure components
├── configs/             # Configuration files
├── pkg/                 # Reusable packages
├── docs/                # API documentation
├── deployments/         # Docker and deployment configs
└── scripts/             # Build and deployment scripts
```

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Language | Go 1.25+ |
| Web Framework | Gin |
| ORM | GORM |
| Database | MySQL (for storing configurations) |
| Security | JWT, SQL Validation, Rate Limiting |
| Configuration | Viper |
| Documentation | Swagger/OpenAPI |
| Containerization | Docker |

## Getting Started

### Prerequisites

- Go 1.25 or higher
- MySQL 5.7+ or 8.0+
- Docker (optional, for containerized deployment)

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

4. **Initialize the database**
   ```bash
   mysql -u root -p < scripts/init.sql
   ```

5. **Run the application**
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

## Configuration

The application uses `configs/config.yaml` for configuration:

```yaml
server:
  port: 8099
  mode: production  # debug, release, production
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
  level: info  # debug, info, warn, error
  format: json
```

## API Usage

### Execute a Query

```bash
curl -X POST http://localhost:8099/api/v1/query \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -d '{
    "dataSourceId": "550e8400-e29b-41d4-a716-446655440000",
    "sql": "SELECT id, name, email FROM users WHERE status = ?",
    "parameters": ["active"],
    "limit": 100,
    "offset": 0
  }'
```

**Response:**
```json
{
  "success": true,
  "data": {
    "columns": [
      {"name": "id", "type": "INTEGER", "nullable": false},
      {"name": "name", "type": "STRING", "nullable": false},
      {"name": "email", "type": "STRING", "nullable": true}
    ],
    "rows": [
      [1, "John Doe", "john@example.com"],
      [2, "Jane Smith", "jane@example.com"]
    ],
    "metadata": {
      "rowCount": 2,
      "executionTimeMs": 45,
      "dataSourceId": "550e8400-e29b-41d4-a716-446655440000",
      "databaseType": "mysql"
    }
  },
  "correlationId": "req_123456789"
}
```

### Create a Data Source

```bash
curl -X POST http://localhost:8099/api/v1/datasources \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -d '{
    "name": "Production MySQL",
    "type": "mysql",
    "config": {
      "host": "prod-mysql.example.com",
      "port": 3306,
      "database": "production",
      "username": "readonly_user",
      "password": "secure_password",
      "maxOpenConns": 100,
      "maxIdleConns": 10,
      "connMaxLifetime": "1h"
    }
  }'
```

### Other Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/health` | Health check |
| GET | `/api/v1/datasources` | List all data sources |
| POST | `/api/v1/datasources` | Create data source |
| GET | `/api/v1/datasources/:id` | Get data source details |
| PUT | `/api/v1/datasources/:id` | Update data source |
| DELETE | `/api/v1/datasources/:id` | Delete data source |
| POST | `/api/v1/query` | Execute SQL query |
| POST | `/api/v1/query/validate` | Validate SQL query |
| GET | `/api/v1/databases/test/:id` | Test database connection |

## Security Features

### SQL Injection Prevention
- Only SELECT statements are allowed
- All queries use parameterized statements
- Query validation before execution

### Authentication & Authorization
- JWT-based authentication
- Configurable token expiration
- Role-based access control (RBAC) ready

### Rate Limiting
- Configurable request limits per time window
- Prevents abuse and DoS attacks

### Audit & Monitoring
- Correlation ID for request tracking
- Query execution logs
- Performance metrics

## Database Schema

```sql
CREATE TABLE data_sources (
    id CHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    type ENUM('mysql','mariadb','postgresql','oracle') NOT NULL,
    config JSON NOT NULL,
    status ENUM('active','inactive','error') DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_type (type),
    INDEX idx_status (status)
);
```

## Development

### Running Tests

```bash
# Run all tests
go test ./...

# Run with coverage
go test -cover ./...

# Generate coverage report
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

### Building

```bash
# Build for current platform
go build -o bin/nexus-gateway cmd/server/main.go

# Build for multiple platforms
./scripts/build.sh
```

### API Documentation

Generate Swagger documentation:

```bash
swag init -g cmd/server/main.go -o docs
```

Access Swagger UI at: `http://localhost:8099/swagger/index.html`

## Production Deployment

### Environment Variables

```bash
export SERVER_MODE=production
export DATABASE_HOST=your-db-host
export DATABASE_PASSWORD=secure-password
export JWT_SECRET=your-jwt-secret
```

### Using Docker Compose

```bash
docker-compose up -d
```

### Kubernetes

```bash
kubectl apply -f deployments/k8s/
```

## Performance Tuning

- **Connection Pooling**: Adjust `maxOpenConns` and `maxIdleConns` based on your workload
- **Query Timeout**: Set appropriate timeout values in configuration
- **Rate Limiting**: Configure rate limits based on your API capacity
- **Caching**: Implement response caching for frequently accessed data

## Monitoring & Observability

Nexus-Gateway provides built-in metrics and monitoring:

- Query execution statistics
- Database connection pool metrics
- API response times
- Error rates and types
- Health check endpoints

Integrate with Prometheus, Grafana, or other monitoring tools using the `/metrics` endpoint.

## Contributing

We welcome contributions! Please follow these steps:

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## Roadmap

### Phase 1: Enhanced Single-Source Capabilities

#### Data Lakes & Warehouses
- [ ] **Table Formats**: Apache Iceberg, Delta Lake, Apache Hudi support
- [ ] **Cloud Data Warehouses**: Snowflake, Databricks, Redshift, BigQuery
- [ ] **OLAP Engines**: ClickHouse, Apache Doris, StarRocks, Apache Druid

#### Object Storage & File Systems
- [ ] **Object Storage**: AWS S3, MinIO, Alibaba OSS, Tencent COS, Azure Blob
- [ ] **Distributed Storage**: HDFS, Apache Ozone
- [ ] **File Formats**: Parquet, ORC, Avro, CSV, JSON, Iceberg, Delta

#### Domestic Database Support (China)
- [ ] **Distributed Databases**: OceanBase, TiDB, Tencent TDSQL, GaussDB
- [ ] **Traditional Databases**: DaMeng (DM), KingbaseES, GBase, Oscar, OpenGauss

### Phase 2: Compute Engine Integration
- [ ] **Trino Integration**: Distributed SQL query engine federation
- [ ] **Spark Integration**: Batch and streaming data processing
- [ ] **Flink Integration**: Real-time stream processing
- [ ] **Compute Engine Orchestration**: Intelligent routing to optimal engine

### Phase 3: Cross-Source Query via Compute Engines
- [ ] **Unified Query API**: Single interface that routes to Trino/Spark for cross-source queries
- [ ] **Source Catalog Management**: Map data sources to Trino/Spark catalogs
- [ ] **Query Translation**: Convert standard SQL to engine-specific syntax when needed
- [ ] **Result Proxy**: Stream results from compute engines back to clients
- [ ] **Engine Health Check**: Monitor compute engine cluster status

### Phase 4: ETL via Compute Engines
- [ ] **ETL Job API**: REST API to submit Spark/Flink ETL jobs
- [ ] **Job Management**: Track job status, logs, and cancellation
- [ ] **Template Library**: Pre-built ETL templates for common patterns (sync, transform, aggregate)
- [ ] **Schedule Integration**: Interface with schedulers (Airflow, DolphinScheduler) for recurring jobs
- [ ] **Visual Pipeline Builder**: Web UI to design ETL pipelines that generate Spark/Flink jobs

### Phase 5: Advanced Features
- [ ] Query result caching with intelligent invalidation
- [ ] GraphQL API support
- [ ] Webhook notifications for query events
- [ ] Advanced analytics dashboard with real-time metrics
- [ ] Multi-region and active-active deployment
- [ ] Query auditing and compliance features
- [ ] AI-powered query optimization and recommendations

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

- Documentation: [Full Docs](https://LuckyCaptain-go.github.io/Nexus-Gateway)
- Issues: [GitHub Issues](https://github.com/LuckyCaptain-go/Nexus-Gateway/issues)
- Discussions: [GitHub Discussions](https://github.com/LuckyCaptain-go/Nexus-Gateway/discussions)

## Acknowledgments

- Built with [Gin](https://gin-gonic.com/) web framework
- ORM powered by [GORM](https://gorm.io/)
- Inspired by the need for unified database access in microservices architectures

---

<div align="center">
Made with ❤️ by the Nexus-Gateway team
</div>
