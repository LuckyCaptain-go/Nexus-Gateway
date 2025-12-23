# Multi-Database Proxy Gateway Specification

## 1. Feature Overview

**Purpose**: Implement a proxy gateway that executes SQL queries against multiple relational database types (MySQL, MariaDB, PostgreSQL, Oracle) using UUID-based data source identification.

**Key Requirements**:
- Support MySQL, MariaDB, PostgreSQL, Oracle databases
- Data sources stored in MySQL with UUID identification
- Standardized result set format across different database types
- Read-only SQL query execution (SELECT statements only)
- SQL injection prevention
- Performance: P95 latency < 100ms for routing decisions

## 2. Functional Requirements

### FR-001: Data Source Management
- Data source configurations stored in MySQL database
- Each data source identified by UUID
- Data source schema includes: UUID, database_type, connection_params, status
- Support dynamic data source lookup by UUID

### FR-002: SQL Query API
- RESTful endpoint: `POST /api/v1/query`
- Request payload: `{ "dataSourceId": "uuid", "sql": "SELECT ..." }`
- SQL validation: Only allow SELECT statements (no INSERT, UPDATE, DELETE, DROP)
- SQL parameter binding support to prevent injection

### FR-003: Database Connectivity
- Support connection pooling for each database type
- Automatic driver selection based on data source type
- Connection health checks and retry logic
- Timeout configuration per query execution

### FR-004: Result Set Standardization
- Unified response format across all database types
- Standardized data type mapping (MySQL types → Standard types)
- Pagination support for large result sets
- Metadata: column names, types, row count

## 3. Non-Functional Requirements

### NFR-001: Performance
- Query routing decision: < 10ms
- Gateway overhead: < 10ms for standard operations
- Support 10,000+ queries per second per instance
- P95 latency: < 100ms total response time

### NFR-002: Security
- SQL injection prevention through parameter binding
- Data source credential encryption
- Audit logging for all queries
- Read-only enforcement (blocking DML/DDL)

### NFR-003: Observability
- Structured logging with correlation IDs
- Metrics: query rates, latency, error rates, connection pool health
- Distributed tracing across database calls
- Health check endpoints

### NFR-004: Reliability
- Horizontal scalability with no shared state
- Circuit breaker pattern for database failures
- Graceful degradation for database unavailability
- Connection pool management with health monitoring

## 4. Technical Constraints

### TC-001: Technology Stack
- Use modern, production-ready database drivers
- Connection pooling: HikariCP or equivalent
- JSON API responses with UTF-8 encoding
- TLS 1.3 for all external communications

### TC-002: Data Source Storage
- Data sources stored in MySQL table: `data_sources`
- Schema: `id (UUID PK)`, `name`, `type`, `config (JSON)`, `status`, `created_at`, `updated_at`
- Config includes: host, port, database, username, encrypted_password

### TC-003: API Versioning
- Header-based versioning: `X-API-Version: 1.0`
- URL-based versioning: `/api/v1/query`
- Backward compatibility maintenance for at least 6 months

## 5. User Stories

### US-001: Query Execution
**As a** client application
**I want to** execute SQL queries against different databases using a data source UUID
**So that** I can retrieve data without managing database connections directly

**Acceptance Criteria**:
- Client provides UUID and SQL query
- System validates and routes to correct database
- Returns standardized JSON result set
- Handles invalid UUID or SQL gracefully

### US-002: Data Source Management
**As an administrator
**I want to** add/update database connection configurations
**So that** clients can reference databases by UUID

**Acceptance Criteria**:
- Admin can configure new data sources with UUID
- System validates connectivity during configuration
- Support MySQL, MariaDB, PostgreSQL, Oracle
- Secure storage of connection credentials

### US-003: Security Enforcement
**As a system owner
**I want to** ensure only read queries are executed
**So that** data integrity is protected

**Acceptance Criteria**:
- Block all non-SELECT SQL statements
- Log all query attempts with source identification
- Return descriptive error messages for blocked queries

## 6. Data Models

### 6.1 Data Source Configuration
```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "Production MySQL",
  "type": "mysql",
  "config": {
    "host": "db.example.com",
    "port": 3306,
    "database": "production",
    "username": "readonly_user",
    "password": "encrypted_password",
    "ssl": true,
    "connectionTimeout": 30000,
    "maxPoolSize": 10
  },
  "status": "active",
  "createdAt": "2025-12-22T10:00:00Z",
  "updatedAt": "2025-12-22T10:00:00Z"
}
```

### 6.2 API Request
```json
{
  "dataSourceId": "550e8400-e29b-41d4-a716-446655440000",
  "sql": "SELECT id, name FROM users WHERE status = ?",
  "parameters": ["active"],
  "limit": 1000,
  "offset": 0
}
```

### 6.3 API Response
```json
{
  "success": true,
  "data": {
    "columns": [
      {"name": "id", "type": "INTEGER", "nullable": false},
      {"name": "name", "type": "STRING", "nullable": false}
    ],
    "rows": [
      [1, "John Doe"],
      [2, "Jane Smith"]
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

## 7. Error Handling

### 7.1 Error Response Format
```json
{
  "success": false,
  "error": {
    "code": "INVALID_SQL",
    "message": "Only SELECT statements are allowed",
    "details": "Attempted to execute INSERT statement"
  },
  "correlationId": "req_123456789"
}
```

### 7.2 Error Codes
- `INVALID_DATA_SOURCE`: UUID not found or inactive
- `INVALID_SQL`: Non-SELECT statement detected
- `SQL_SYNTAX_ERROR`: Invalid SQL syntax
- `CONNECTION_ERROR`: Database connection failed
- `TIMEOUT_ERROR`: Query execution timeout
- `UNAUTHORIZED`: Invalid authentication
- `RATE_LIMIT_EXCEEDED`: Too many requests

## 8. Security Requirements

### 8.1 SQL Injection Prevention
- Parameterized queries only (no string concatenation)
- SQL statement validation before execution
- Block dangerous SQL keywords: DROP, DELETE, UPDATE, INSERT, ALTER

### 8.2 Authentication & Authorization
- API key or token-based authentication
- Data source access control per client
- Audit trail for all query executions

### 8.3 Data Protection
- Encrypt database credentials at rest
- Use TLS for all communications
- Never log sensitive data (passwords, PII)

## 9. Integration Points

### 9.1 External Dependencies
- MySQL database for data source storage
- Database drivers: MySQL, PostgreSQL, Oracle
- Authentication service (optional)
- Monitoring/metrics system

### 9.2 API Contracts
- OpenAPI 3.0 specification for query endpoint
- Data source management API endpoints
- Health check and metrics endpoints

## 10. Deployment Considerations

### 10.1 Configuration Management
- Environment-specific database connections
- Externalize all configuration properties
- Support hot-reload of configuration changes

### 10.2 Scaling
- Horizontal scaling with load balancer
- Stateless design for easy scaling
- Database connection pooling per instance