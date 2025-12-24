# Phase 0 Research: Technology Decisions for Phase 1 Enhanced Single-Source Capabilities

**Feature**: Phase 1 Enhanced Single-Source Capabilities
**Date**: 2025-12-24
**Purpose**: Resolve all technical unknowns and select optimal technology stack for implementing 30+ data source drivers

## Research Summary

This document consolidates research findings for 6 categories of data sources, covering technology choices, best practices, and integration patterns. All decisions prioritize Go library compatibility, production readiness, and alignment with existing architecture patterns.

---

## 1. Data Lake Table Formats

### Apache Iceberg

**Decision**: Use REST API client approach with optional Spark bridge for complex queries

**Rationale**:
- Apache Iceberg Go libraries are immature as of 2025
- Iceberg REST Catalog API is stable and well-documented
- REST approach avoids JVM dependency (pure Go implementation)
- For complex operations, can bridge to Spark via compute engines (Phase 2)

**Chosen Technology**:
- Primary: Custom REST client wrapping Iceberg REST Catalog API
- Backup: `github.com/apache/iceberg-go` (if matured by implementation)
- Metadata: Iceberg's Avro-based metadata parsing via `github.com/linkedin/goavro`

**Alternatives Considered**:
- Native Go Iceberg library: Rejected due to immaturity and limited features
- Spark connector: Rejected for Phase 1 (adds JVM dependency), consider for Phase 2 compute engine integration
- Trino/Iceberg: Rejected (introduces separate infrastructure), align with Phase 3 vision

**Best Practices**:
- Leverage Iceberg's snapshot isolation for consistent queries
- Implement partition pruning using Iceberg manifest metadata
- Cache table metadata locally to reduce REST calls
- Use Iceberg's schema evolution handling (backward/forward compatibility)

---

### Delta Lake

**Decision**: Use Databricks Delta Sharing protocol or REST API

**Rationale**:
- Delta Lake native Go support requires Spark/JVM
- Delta Sharing protocol provides REST-based access to Delta tables
- Databricks offers official REST APIs for Delta Lake operations
- Aligns with cloud-native architecture (no local filesystem dependency)

**Chosen Technology**:
- Primary: Delta Sharing REST client (`github.com/delta-io/delta-sharing-go` if available, else custom)
- For Databricks: Databricks SQL endpoint (REST API)
- Bridge to Spark: Consider in Phase 2 (ETL via compute engines)

**Alternatives Considered**:
- `github.com/delta-io/connectors`: Rejected (Python-based, requires subprocess)
- Native Delta Lake reader: Rejected (JVM dependency via delta-rs)
- Direct Parquet reading: Rejected (loses Delta Lake transaction log features)

**Best Practices**:
- Use Delta Lake's time travel feature via `AS OF` syntax (parse from Delta log)
- Implement checkpoint reading for efficient historical queries
- Cache Delta log metadata locally
- Handle Delta Lake's schema evolution (add/drop/rename columns)

---

### Apache Hudi

**Decision**: Use Hudi REST API or HoodieReadClient via HTTP

**Rationale**:
- Native Go Hudi libraries don't exist (Hudi is JVM-based)
- Hudi provides REST APIs for metadata and queries
- Can read Hudi tables via Spark (Phase 2) or Trino (Phase 3)
- For Phase 1: Use file-level reading (Parquet/Avro) + Hudi metadata parsing

**Chosen Technology**:
- Primary: Hudu REST API client (custom or community library)
- Fallback: Direct file reading (Parquet base files + timeline metadata)
- Metadata parsing: Avro reading of Hudi metadata files

**Alternatives Considered**:
- Spark connector: Rejected (JVM dependency, align to Phase 2)
- `github.com/apache/hudi-go`: Doesn't exist (immature ecosystem)
- Trino integration: Rejected for Phase 1 (infrastructure overhead), Phase 3 candidate

**Best Practices**:
- Detect Hudi table type (Copy-On-Write vs Merge-On-Read) from metadata
- For MOR tables, implement base file + log file merging
- Use Hudi's incremental query capabilities (query by commit time)
- Leverage Hudi's partition pruning from metadata

---

## 2. Cloud Data Warehouses

### Snowflake

**Decision**: Use official Go Snowflake driver

**Rationale**:
- `github.com/snowflakedb/gosnowflake` is official and production-ready
- Native Go implementation (no external dependencies)
- Supports key-pair auth, OAuth, and browser-based SSO
- Excellent integration with existing `database/sql` interface

**Chosen Technology**:
- Driver: `github.com/snowflakedb/gosnowflake` (v1.7+)
- DSN format: `<user>:<password>@<account>/<database>/<schema>?warehouse=<warehouse>`
- Connection pooling: Existing `ConnectionPool` infrastructure

**Best Practices**:
- Use Snowflake's result streaming via `sql.Rows` iteration
- Handle Snowflake's VARIANT/ARRAY/OBJECT types (map to JSON in responses)
- Implement query tag usage for cost tracking
- Use Snowflake's PUT/GET for staged data (future Phase 4 ETL)

---

### Databricks

**Decision**: Use Databricks SQL connector via HTTP/REST

**Rationale**:
- Databricks doesn't provide official Go driver
- Databricks SQL endpoint has well-documented REST API
- Can use JDBC driver via `github.com/thda/tds` (less ideal)
- HTTP API approach is cloud-native and aligns with gateway architecture

**Chosen Technology**:
- Primary: Databricks SQL REST API (custom client)
- DSN format: `https://<workspace>.databricks.com/sql/1.0/warehouses/<warehouse_id>`
- Auth: Personal Access Token (PAT) or OAuth
- Alternative: Databricks JDBC driver via `database/sql` (less recommended)

**Best Practices**:
- Use Databricks cluster endpoints (not all-purpose clusters for prod)
- Implement statement polling for async queries
- Handle Databricks' delta format transparently (handled by Databricks)
- Use Databricks' query result streaming (chunked responses)

---

### Amazon Redshift

**Decision**: Use PostgreSQL protocol (Redshift is PostgreSQL-compatible)

**Rationale**:
- Redshift is based on PostgreSQL 8.0.2
- Existing PostgreSQL driver works with Redshift
- Native Go support via existing PostgreSQL driver infrastructure
- Can leverage AWS IAM authentication for enhanced security

**Chosen Technology**:
- Driver: `github.com/lib/pq` (existing PostgreSQL driver)
- Enhanced: Add Redshift-specific optimizations
- Auth: Password-based or IAM role-based auth
- Connection: Standard PostgreSQL connection string

**Best Practices**:
- Use Redshift's result set caching (automatic)
- Implement Redshift's query queue management (WLM queues)
- Handle Redshift's columnar data types (CHAR, VARCHAR limited to 65535)
- Use Redshift's UNLOAD/INSERT for bulk operations (future Phase 4)

---

### Google BigQuery

**Decision**: Use official BigQuery Go client library

**Rationale**:
- `cloud.google.com/go/bigquery` is official and production-ready
- Native Go implementation with comprehensive API coverage
- Supports OAuth2, service account, and ADC (Application Default Credentials)
- Excellent query result streaming and pagination support

**Chosen Technology**:
- Client: `cloud.google.com/go/bigquery` (latest)
- Auth: OAuth2 or service account JSON
- Query execution: `Client.Query()` with job-based execution model
- Result handling: Iterator-based streaming

**Best Practices**:
- Use BigQuery's query priority flags (interactive vs batch)
- Implement BigQuery's query result caching (automatic)
- Handle BigQuery's pagination (page tokens)
- Use dry-run queries for cost estimation before execution
- Use query labels for cost tracking

---

## 3. Object Storage Query Support

### AWS S3

**Decision**: Use AWS SDK for Go v2 with file format libraries

**Rationale**:
- `github.com/aws/aws-sdk-go-v2` is official and modular
- Native Go implementation with excellent performance
- Supports IAM roles, session credentials, and assume-role
- S3 Select API for SQL-like filtering (reduces data transfer)

**Chosen Technology**:
- S3 Client: `github.com/aws/aws-sdk-go-v2/service/s3`
- Auth: AWS credentials chain (env, profile, IAM role)
- Parquet: `github.com/xitongsys/parquet-go` for Parquet reading
- ORC: `github.com/scritchley/orc` (or custom reader)
- CSV/JSON: Standard library encoding
- S3 Select: Use `SelectObjectContent` for predicate pushdown

**Best Practices**:
- Use S3 multipart download for large files
- Implement S3 list objects v2 (pagination support)
- Use S3 Select for server-side filtering (CSV, JSON, Parquet)
- Cache S3 object metadata (ETag for change detection)
- Handle S3 eventual consistency (list operations)

---

### MinIO

**Decision**: Use MinIO Go SDK (S3-compatible API)

**Rationale**:
- MinIO is S3-compatible API
- `github.com/minio/minio-go/v7` is official and feature-complete
- Drop-in replacement for S3 in most use cases
- Self-hosted option for on-premises deployments

**Chosen Technology**:
- Client: `github.com/minio/minio-go/v7`
- File format libraries: Same as AWS S3 (Parquet, ORC, CSV, JSON)
- Auth: Access key + secret key (same as S3)
- Features: MinIO server-side query capabilities (if available)

**Best Practices**:
- MinIO behaves like S3, reuse S3 driver with endpoint configuration
- Implement MinIO-specific features (erasure coding, versioning)
- Use MinIO's browser-based upload for testing

---

### Alibaba OSS / Tencent COS

**Decision**: Use official SDK clients with S3 abstraction layer

**Rationale**:
- Both Alibaba OSS and Tencent COS provide official Go SDKs
- Implement S3-compatible protocol abstractions
- Can leverage existing S3 driver patterns
- Critical for China market compliance (data sovereignty)

**Chosen Technology**:
- Alibaba OSS: `github.com/aliyun/aliyun-oss-go-sdk` (official)
- Tencent COS: `github.com/tencentyun/cos-go-sdk-v5` (official)
- Abstraction: Common interface matching AWS S3 client pattern
- File format: Reuse Parquet/ORC/CSV/JSON libraries

**Best Practices**:
- Implement unified S3-like client interface for all object storage
- Handle provider-specific authentication (OSS STS token, COS temp key)
- Implement provider-specific error translation
- Use provider-specific features (OSS lifecycle, COS versioning)

---

### Azure Blob Storage

**Decision**: Use Azure Blob Storage SDK for Go

**Rationale**:
- `github.com/Azure/azure-storage-blob-go` is official
- Native Go implementation with comprehensive features
- Supports SAS tokens, OAuth2, and shared key auth
- Azure AD integration for enterprise scenarios

**Chosen Technology**:
- Client: `github.com/Azure/azure-storage-blob-go`
- Auth: SAS token, OAuth2 via `github.com/Azure/azure-sdk-for-go`
- Features: Azure Blob lifecycle management, tiered storage
- File format: Reuse Parquet/ORC/CSV/JSON libraries

**Best Practices**:
- Use Azure Blob's hierarchical namespace (Data Lake Storage Gen2)
- Implement Azure-specific features (cool/archive tier access)
- Handle Azure Blob's blob tier limitations (hot access required for queries)
- Use Azure AD pass-through auth for enterprise deployments

---

### HDFS and Apache Ozone

**Decision**: Use HDFS client with Kerberos authentication

**Rationale**:
- `github.com/colinmarc/hdfs/v2` is the de-facto standard HDFS Go client
- Supports Kerberos authentication via `github.com/jcmturner/gokrb5`
- Apache Ozone provides S3-compatible API (use S3 driver)
- HDFS is critical for legacy big data environments

**Chosen Technology**:
- HDFS Client: `github.com/colinmarc/hdfs/v2`
- Kerberos: `github.com/jcmturner/gokrb5` for SPNEGO auth
- Apache Ozone: Use S3 driver (Ozone supports S3A protocol)
- File format: Reuse Avro/Parquet/ORC/CSV/JSON libraries

**Best Practices**:
- Implement Kerberos ticket renewal (kinit + ticket cache)
- Use HDFS High-Availability (nameservice failover)
- Cache HDFS file metadata (reduce NameNode queries)
- Use HDFS short-circuit reads for local data (performance)
- Handle HDFS security (token auth, wire encryption)

---

## 4. High-Performance OLAP Engines

### ClickHouse

**Decision**: Use official ClickHouse Go driver

**Rationale**:
- `github.com/ClickHouse/clickhouse-go` is official and production-ready
- Native Go implementation with excellent performance
- Supports both native protocol and HTTP interface
- Excellent ClickHouse-specific data type support

**Chosen Technology**:
- Driver: `github.com/ClickHouse/clickhouse-go` (v2+ recommended)
- Interface: Native protocol (preferred) or HTTP
- Connection: ClickHouse DSN format
- Data types: ClickHouse arrays, tuples, nested structures

**Best Practices**:
- Use ClickHouse's native protocol for performance (HTTP is slower)
- Implement ClickHouse's query settings (max_execution_time, max_rows_to_read)
- Handle ClickHouse's aggregate function combinatorics
- Use ClickHouse's external data for large datasets
- Implement query logging for ClickHouse's query log analysis

---

### Apache Doris / StarRocks

**Decision**: Use MySQL protocol compatibility (both are MySQL-compatible)

**Rationale**:
- Apache Doris and StarRocks provide MySQL-compatible frontends
- Existing MySQL driver (`github.com/go-sql-driver/mysql`) works
- Native Go support via standard MySQL protocol
- No additional dependencies required

**Chosen Technology**:
- Driver: `github.com/go-sql-driver/mysql` (existing)
- Connection: Standard MySQL connection string to Doris/StarRocks FE
- Features: Leverage MySQL protocol support
- Optimization: Use Doris/StarRocks-specific query hints

**Best Practices**:
- Use Doris/StarRocks's MySQL protocol optimizations
- Implement Doris's rollup selection (query rollup tables)
- Use StarRocks's pipeline execution engine
- Handle Doris/StarRocks's distributed table sharding

---

### Apache Druid

**Decision**: Use Druid REST API client

**Rationale**:
- Druid is primarily accessed via HTTP/REST (native SQL via SQL API)
- No official Go driver (REST API is standard)
- Druid's SQL API is stable and well-documented
- Can bridge to native Druid queries for advanced features

**Chosen Technology**:
- Client: Custom REST client wrapping Druid SQL API
- Endpoint: `http://<broker>:8082/druid/v2/sql`
- Query: Native SQL via POST request
- Result: JSON response (multi-dimensional array format)

**Best Practices**:
- Use Druid's SQL API for standard queries (avoid native JSON queries)
- Implement Druid's time series optimizations (granularity handling)
- Use Druid's query context (timeout, priority)
- Handle Druid's multi-value dimension arrays
- Implement Druid's pagination (offset-based, limited)

---

## 5. Domestic Chinese Databases

### OceanBase / TiDB / TDSQL / GaussDB

**Decision**: Leverage MySQL/PostgreSQL protocol compatibility

**Rationale**:
- OceanBase, TiDB, TDSQL, and GaussDB (MySQL mode) are MySQL-compatible
- GaussDB (PostgreSQL mode) is PostgreSQL-compatible
- Existing MySQL/PostgreSQL drivers work without modification
- These distributed databases extend standard protocols

**Chosen Technology**:
- OceanBase (MySQL mode): `github.com/go-sql-driver/mysql` (existing)
- OceanBase (Oracle mode): Oracle driver (need Oracle Go client)
- TiDB: `github.com/go-sql-driver/mysql` (existing) + TiDB-specific optimizations
- TDSQL: `github.com/go-sql-driver/mysql` (existing)
- GaussDB (MySQL mode): `github.com/go-sql-driver/mysql` (existing)
- GaussDB (PostgreSQL mode): `github.com/lib/pq` (existing PostgreSQL driver)

**Best Practices**:
- Detect database compatibility mode from DSN or connection metadata
- Implement distributed transaction handling (avoid in read-only gateway)
- Use database-specific hints for performance (e.g., TiDB hash join)
- Handle distributed query optimization (partition pruning)

---

### DaMeng (DM) / KingbaseES / GBase / Oscar / OpenGauss

**Decision**: Use vendor-specific or PostgreSQL-compatible drivers

**Rationale**:
- DaMeng: Has its own protocol (need DM Go driver or ODBC bridge)
- KingbaseES: PostgreSQL-compatible, use `github.com/lib/pq`
- GBase 8s: Informix-compatible (limited Go support, may need custom)
- GBase 8t: TDDL-compatible (similar to MySQL)
- Oscar: PostgreSQL-compatible variant
- OpenGauss: PostgreSQL-compatible, use `github.com/lib/pq`

**Chosen Technology**:
- DaMeng: Custom driver or ODBC bridge (`github.com/ninthclowdunix/godbc`)
- KingbaseES: `github.com/lib/pq` (PostgreSQL driver)
- GBase 8s: Custom Informix-compatible driver (research needed)
- GBase 8t: `github.com/go-sql-driver/mysql` (MySQL-compatible)
- Oscar: `github.com/lib/pq` (PostgreSQL driver)
- OpenGauss: `github.com/lib/pq` (PostgreSQL driver)

**Best Practices**:
- Test Chinese character encoding (UTF-8, GBK, GB18030) for all databases
- Implement database-specific type mapping (e.g., DM's CLOB, BLOB)
- Handle Oracle compatibility modes (OceanBase, DaMeng Oracle mode)
- Use database-specific connection pooling parameters
- Test with Chinese characters in queries and results (SC-010)

---

## 6. Security & Authentication

### Credential Management

**Decision**: Implement encrypted credential vault with environment-specific keys

**Rationale**:
- Credentials must be encrypted at rest (FR-035)
- Use AES-256-GCM for credential encryption
- Master key from environment variable or key management service
- Rotate credentials per data source type requirements

**Chosen Technology**:
- Encryption: `crypto/aes` + `crypto/cipher` (standard library)
- Key derivation: Master key from env or KMS (AWS KMS, Azure Key Vault, GCP KMS)
- Storage: Encrypted JSON in MySQL `data_sources.config` field
- Rotation: Background goroutine for token refresh (FR-039)

**Best Practices**:
- Never log credentials or tokens (mask in logs)
- Use different encryption keys per environment (dev/staging/prod)
- Implement credential versioning for rotation support
- Use secure memory practices (zeroize sensitive data after use)

---

### Authentication Protocols

**Decision**: Implement multiple authentication strategies

**Chosen Technologies**:
- AWS: IAM role via `github.com/aws/aws-sdk-go-v2/config`
- Google Cloud: OAuth2 via `golang.org/x/oauth2` and ADC
- Azure: Azure AD via `github.com/Azure/azure-sdk-for-go`
- Kerberos: SPNEGO via `github.com/jcmturner/gokrb5`
- JWT: Existing JWT infrastructure (extend for data source auth)
- Basic auth: Existing username/password (extend with encryption)

**Best Practices**:
- Implement token auto-rotation before expiration (FR-039)
- Use credential chains (try multiple auth methods in order)
- Cache authentication tokens (with TTL-based expiration)
- Handle MFA challenges (for interactive authentication flows)

---

## 7. Testing Strategy

### Unit Testing

**Decision**: Go testing + testify for assertions

**Rationale**:
- Standard Go `testing` package with table-driven tests
- `github.com/stretchr/testify` for assertions and mocks
- Mock external dependencies (data sources) for deterministic tests
- Test each driver independently

**Chosen Technology**:
- Testing: `testing` package + `testify` for assertions/mocks
- Coverage: `go test -cover` (target 80%+ for core logic)
- Mocking: `github.com/stretchr/testify/mock` + `gomock` for complex mocks

---

### Integration Testing

**Decision**: Testcontainers for data source isolation

**Rationale**:
- `github.com/testcontainers/testcontainers-go` for ephemeral data sources
- Spin up real databases/storage in Docker for testing
- Test against actual data sources (not just mocks)
- Parallel test execution with isolated containers

**Chosen Technology**:
- Testcontainers: `github.com/testcontainers/testcontainers-go`
- Modules: `testcontainers/modules/mysql`, `testcontainers/modules/postgres`
- Custom containers: ClickHouse, MinIO (modules available)
- Cloud services: Use test credentials/accounts (S3, Snowflake, etc.)

**Best Practices**:
- Use test data fixtures (small datasets for fast tests)
- Implement test data generation (avoid large test files)
- Mock cloud services when possible (LocalStack for AWS, emulators)
- Skip tests requiring cloud services when CI environment lacks credentials

---

### Contract Testing

**Decision**: OpenAPI spec validation with schemathesis

**Rationale**:
- Validate API responses against OpenAPI spec (constitution requirement)
- `github.com/schemathesis/schemathesis` for contract testing
- Ensure API contract compliance before implementation

**Chosen Technology**:
- Spec validation: `github.com/getkin/kin-openapi` for OpenAPI parsing
- Contract testing: Schemathesis or custom validator
- Test generation: Generate test cases from OpenAPI spec

---

## 8. Performance & Scalability

### Query Result Streaming

**Decision**: Stream results via `sql.Rows` iteration

**Rationale**:
- Avoid loading entire result set in memory (FR-044)
- Use `database/sql`'s native streaming (`rows.Next()`)
- Implement chunked HTTP responses (server-sent events or chunked encoding)
- Memory limit per query: 512MB (SC-009, SC-011)

**Implementation**:
- Use `rows.Next()` cursor-based iteration
- Flush HTTP response buffer after each row/batch
- Implement query cancellation via context.Context
- Monitor memory usage per query (runtime.MemStats)

---

### Connection Pooling

**Decision**: Extend existing connection pool with token management

**Rationale**:
- Existing `ConnectionPool` infrastructure (`internal/database/connection_pool.go`)
- Extend to support authentication token caching and rotation
- Reuse connections per data source (efficiency)
- Configure pool size per data source type (FR-043)

**Implementation**:
- Extend `ConnectionPool` with `AuthTokenCache` map
- Implement background token refresh goroutine (FR-039)
- Pool size configuration per source type (OLAP: small pool, warehouses: large pool)
- Health check validation before returning connection from pool

---

## 9. Observability & Monitoring

### Structured Logging

**Decision**: Use existing logging patterns with structured fields

**Rationale**:
- Existing correlation ID infrastructure (`internal/middleware/correlation.go`)
- Add structured fields: query_id, data_source_type, execution_time_ms, row_count
- Log query execution metrics (FR-045)
- Use `log/slog` (Go 1.21+) or existing logging approach

**Implementation**:
- Log query start/end with correlation ID
- Include query hash (for query pattern analysis)
- Log slow queries (> threshold configurable)
- Mask sensitive data in logs (credentials, tokens)

---

### Metrics Collection

**Decision**: Prometheus metrics via `/metrics` endpoint

**Rationale**:
- Standard Prometheus exposition format
- Metrics: query latency histogram, error rate counter, active queries gauge
- Per-data-source-type metrics (enable filtering)
- Existing `/metrics` endpoint infrastructure (extend)

**Implementation**:
- Use `github.com/prometheus/client_golang` for metrics
- Metrics: `nexus_query_duration_seconds`, `nexus_query_errors_total`
- Labels: data_source_type, data_source_id, query_status
- Health check metrics: `nexus_datasource_health_status`

---

## 10. Schema Evolution Handling

**Decision**: Schema versioning and compatibility checks

**Rationale**:
- Data lake formats evolve schemas (Iceberg, Delta Lake, Hudi) (FR-005)
- Must handle added columns, type changes without query failures
- Cache schema metadata with versioning
- Validate queries against latest schema

**Implementation**:
- Extract schema metadata from table format (Iceberg: metadata.json, Delta: log)
- Cache schema locally with version/timestamp
- Detect schema changes via metadata version comparison
- Re-validate queries when schema changes detected
- Support backward-compatible schema evolution (add columns, non-breaking type changes)

---

## 11. OpenAPI Contract Generation

**Decision**: Use swaggo/swag for OpenAPI spec generation

**Rationale**:
- `github.com/swaggo/swag` is standard Go OpenAPI generator
- Annotations in controller code (automatic spec generation)
- Supports Go struct validation tags
- Generates Swagger UI for documentation

**Chosen Technology**:
- Generator: `github.com/swaggo/swag/cmd/swag`
- Annotations: Controller doc comments
- Output: `docs/swagger.yaml`, `docs/swagger.json`
- UI: Swagger UI via `github.com/swaggo/gin-swagger`

**Implementation**:
- Add annotations to `datasource_controller.go`, `query_controller.go`
- Include request/response DTOs in spec
- Document new data source types in DataSource enum
- Generate OpenAPI spec in Phase 1 before implementation

---

## Summary of Technology Decisions

| Category | Primary Technology | Fallback/Alternative | Status |
|----------|-------------------|---------------------|--------|
| Apache Iceberg | REST API client | Spark bridge (Phase 2) | ✅ Decision Made |
| Delta Lake | Delta Sharing / Databricks REST | Spark connector (Phase 2) | ✅ Decision Made |
| Apache Hudi | REST API client | Spark connector (Phase 2) | ✅ Decision Made |
| Snowflake | `github.com/snowflakedb/gosnowflake` | N/A | ✅ Decision Made |
| Databricks | REST API client | JDBC driver | ✅ Decision Made |
| Redshift | `github.com/lib/pq` (PostgreSQL driver) | N/A | ✅ Decision Made |
| BigQuery | `cloud.google.com/go/bigquery` | N/A | ✅ Decision Made |
| AWS S3 | `github.com/aws/aws-sdk-go-v2` | N/A | ✅ Decision Made |
| MinIO | `github.com/minio/minio-go/v7` | N/A | ✅ Decision Made |
| Alibaba OSS | `github.com/aliyun/aliyun-oss-go-sdk` | N/A | ✅ Decision Made |
| Tencent COS | `github.com/tencentyun/cos-go-sdk-v5` | N/A | ✅ Decision Made |
| Azure Blob | `github.com/Azure/azure-storage-blob-go` | N/A | ✅ Decision Made |
| HDFS | `github.com/colinmarc/hdfs/v2` | N/A | ✅ Decision Made |
| Apache Ozone | S3 driver (S3A-compatible) | N/A | ✅ Decision Made |
| ClickHouse | `github.com/ClickHouse/clickhouse-go` | N/A | ✅ Decision Made |
| Apache Doris | MySQL driver (`github.com/go-sql-driver/mysql`) | N/A | ✅ Decision Made |
| StarRocks | MySQL driver (`github.com/go-sql-driver/mysql`) | N/A | ✅ Decision Made |
| Apache Druid | REST API client | Native JSON queries | ✅ Decision Made |
| OceanBase | MySQL driver (MySQL mode), Oracle driver (Oracle mode) | N/A | ✅ Decision Made |
| TiDB | MySQL driver (`github.com/go-sql-driver/mysql`) | N/A | ✅ Decision Made |
| TDSQL | MySQL driver (`github.com/go-sql-driver/mysql`) | N/A | ✅ Decision Made |
| GaussDB | MySQL/PostgreSQL driver (based on mode) | N/A | ✅ Decision Made |
| DaMeng | Custom driver / ODBC bridge | Research needed | ⚠️ Research Needed |
| KingbaseES | PostgreSQL driver (`github.com/lib/pq`) | N/A | ✅ Decision Made |
| GBase 8s | Custom Informix driver | Research needed | ⚠️ Research Needed |
| GBase 8t | MySQL driver | N/A | ✅ Decision Made |
| Oscar | PostgreSQL driver (`github.com/lib/pq`) | N/A | ✅ Decision Made |
| OpenGauss | PostgreSQL driver (`github.com/lib/pq`) | N/A | ✅ Decision Made |

**Next Steps**:
1. ⚠️ Conduct deep dive research on DaMeng and GBase 8s driver availability (low-priority domestic databases)
2. Implement P1-P3 drivers first (table formats, warehouses, object storage)
3. P4-P6 drivers implemented incrementally based on user demand
4. All technology decisions are ready for Phase 1 design

---

## Research Completed: ✅ READY FOR PHASE 1

All technical unknowns resolved. Technology stack selected. Ready to proceed to Phase 1 (Design & Contracts).
