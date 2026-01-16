# Architecture Guide

## Overview

Nexus-Gateway follows a clean, modular architecture designed for extensibility and maintainability. The system is built around a core gateway that routes queries to appropriate database drivers based on data source configuration.

## High-Level Architecture

```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────────┐
│   Client Apps   │───▶│  Nexus-Gateway   │───▶│  Data Sources       │
│                 │    │                  │    │                     │
│ - BI Tools      │    │ - Query Routing  │    │ - Cloud Warehouses  │
│ - Dashboards    │    │ - Auth/Z          │    │ - Data Lakes        │
│ - Scripts       │    │ - Connection Pool│    │ - OLAP Engines      │
│ - etc.          │    │ - Security       │    │ - Object Storage    │
└─────────────────┘    │ - Monitoring     │    │ - File Systems      │
                       └──────────────────┘    │ - Domestic DBs      │
                                               └─────────────────────┘
```

## Internal Architecture

### Core Components

```
Nexus-Gateway/
├── cmd/server/                    # Application entry point
├── internal/
│   ├── controller/               # HTTP request handlers
│   │   ├── query_controller.go
│   │   ├── datasource_controller.go
│   │   ├── health_controller.go
│   │   └── ...
│   ├── service/                  # Business logic layer
│   │   ├── query_service.go
│   │   ├── datasource_service.go
│   │   ├── driver_service.go
│   │   └── ...
│   ├── repository/               # Data access layer
│   │   ├── datasource_repository.go
│   │   └── ...
│   ├── model/                    # Data models and DTOs
│   │   ├── datasource.go
│   │   ├── query.go
│   │   └── ...
│   ├── middleware/               # HTTP middleware (JWT, rate limiting, etc.)
│   │   ├── auth_middleware.go
│   │   ├── rate_limit.go
│   │   └── ...
│   └── database/
│       ├── connection_pool.go    # Connection pooling
│       ├── driver_registry.go    # Driver registration system
│       ├── health_checker.go     # Health check utilities
│       └── drivers/              # Database drivers (90+ implementations)
│           ├── common/           # Shared driver utilities
│           │   └── types.go
│           ├── table_format/     # Iceberg, Delta Lake, Hudi
│           ├── warehouses/       # Snowflake, Databricks, Redshift, BigQuery
│           ├── olap/             # ClickHouse, Doris, StarRocks, Druid
│           ├── object_storage/   # S3, MinIO, OSS, COS, Azure Blob
│           ├── file_system/      # HDFS, Apache Ozone
│           └── domestic/         # OceanBase, TiDB, TDSQL, etc.
├── configs/                      # Configuration files
├── pkg/response/                 # Response formatting utilities
├── migrations/                   # Database migration scripts
└── specs/                        # Specification documents
```

### Key Design Patterns

#### 1. Driver Registry Pattern
The system uses a centralized registry to manage different database drivers:

```go
type DriverRegistry struct {
    drivers map[model.DatabaseType]func() drivers.Driver
    mutex   sync.RWMutex
}

func (dr *DriverRegistry) register(dbType model.DatabaseType, factoryFunc func() drivers.Driver) {
    dr.mutex.Lock()
    defer dr.mutex.Unlock()
    dr.drivers[dbType] = factoryFunc
}

func (dr *DriverRegistry) GetDriver(dbType model.DatabaseType) (drivers.Driver, error) {
    dr.mutex.RLock()
    defer dr.mutex.RUnlock()
    
    factoryFunc, exists := dr.drivers[dbType]
    if !exists {
        return nil, fmt.Errorf("unsupported database type: %s", dbType)
    }
    
    return factoryFunc(), nil
}
```

#### 2. Connection Pooling
Each data source maintains its own connection pool to optimize resource usage:

- Separate pools per data source type
- Configurable max connections
- Health checks and automatic recovery
- Reuse of existing connections

#### 3. Query Routing
The system routes queries based on data source UUID:

1. Receive query with dataSourceId
2. Look up data source configuration
3. Validate query (only SELECT statements)
4. Route to appropriate driver
5. Execute query and return results

## Security Architecture

### Authentication Flow
```
Client Request → JWT Validation → Data Source ACL → Query Validation → Execution
```

### Security Controls
- JWT-based authentication
- SQL injection prevention
- Connection credential encryption
- Rate limiting
- Query validation (SELECT only)
- Data source access controls

## Performance Architecture

### Caching Strategy
- Query result caching (optional)
- Connection pooling
- Schema caching
- Prepared statement caching

### Streaming Support
- Server-side cursors for large result sets
- Chunked response delivery
- Configurable batch sizes
- Automatic fallback to LIMIT/OFFSET

## Extensibility Points

### Adding New Drivers
The system is designed to easily accommodate new database types:

1. Implement the `drivers.Driver` interface
2. Register the driver in the registry
3. Add configuration support
4. Update documentation

### Middleware Extensions
New middleware can be added to enhance functionality:
- Authentication methods
- Logging enhancements
- Performance monitoring
- Custom validation rules

## Deployment Architecture

### Single Instance
```
┌─────────────────┐
│   Load Balancer │
└─────────┬───────┘
          │
    ┌─────▼──────┐
    │  Gateway   │
    │  Instance  │
    └─────┬──────┘
          │
    ┌─────▼─────────────┐
    │   Data Sources    │
    └───────────────────┘
```

### Multi-Instance
```
┌─────────────────┐
│   Load Balancer │
└─────────┬───────┘
          │
    ┌─────▼─────────────┐
    │  Gateway Cluster  │
    ├───────────────────┤
    │ GW-1 │ GW-2 │ GW-3 │
    └──────┴──────┴─────┘
          │
    ┌─────▼─────────────┐
    │   Data Sources    │
    └───────────────────┘
```

## Monitoring and Observability

### Metrics Collection
- Query execution times
- Connection pool metrics
- Error rates
- Throughput measurements
- Resource utilization

### Logging
- Structured JSON logs
- Correlation IDs for request tracing
- Detailed audit trails
- Performance insights