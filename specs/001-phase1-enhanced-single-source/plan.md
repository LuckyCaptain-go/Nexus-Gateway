# Implementation Plan: Phase 1 Enhanced Single-Source Capabilities

**Branch**: `001-phase1-enhanced-single-source` | **Date**: 2025-12-24 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `/specs/001-phase1-enhanced-single-source/spec.md`

**Note**: This template is filled in by the `/speckit.plan` command. See `.specify/templates/commands/plan.md` for the execution workflow.

## Summary

Phase 1 extends Nexus-Gateway to support modern data platforms beyond traditional relational databases. The primary requirement is to enable read-only SQL query execution against 6 categories of data sources: (1) Data lake table formats (Apache Iceberg, Delta Lake, Apache Hudi), (2) Cloud data warehouses (Snowflake, Databricks, Redshift, BigQuery), (3) Object storage query engines (AWS S3, MinIO, Alibaba OSS, Tencent COS, Azure Blob), (4) High-performance OLAP engines (ClickHouse, Apache Doris, StarRocks, Druid), (5) Domestic Chinese databases (OceanBase, TiDB, TDSQL, GaussDB, DaMeng, KingbaseES, GBase, Oscar, OpenGauss), and (6) Distributed file systems (HDFS, Apache Ozone).

The technical approach leverages a pluggable driver architecture (pattern established in existing `internal/database/drivers.go`) with abstraction layers for query execution, connection pooling, authentication, and schema metadata. Each data source category requires specialized drivers that understand the underlying protocol (HTTP, native database protocol, object storage APIs) and can translate standard SQL queries into source-specific execution patterns.

## Technical Context

**Language/Version**: Go 1.25+ (existing stack)
**Primary Dependencies**:
- Existing: Gin (web framework), GORM (ORM), JWT (auth), Viper (config)
- New Phase 1 dependencies (determined in research):
  - Apache Iceberg: `github.com/apache/iceberg-go` or REST API client
  - Delta Lake: `github.com/delta-io/connectors` or Spark/Databricks connector
  - Apache Hudi: `github.com/apache/hudi-go` or REST API client
  - Snowflake: `github.com/snowflakedb/gosnowflake` (official driver)
  - BigQuery: `cloud.google.com/go/bigquery` (official client)
  - AWS S3/Redshift: `github.com/aws/aws-sdk-go-v2`
  - Azure Blob: `github.com/Azure/azure-storage-blob-go`
  - Alibaba OSS/Tencent COS: Official SDK clients
  - ClickHouse: `github.com/ClickHouse/clickhouse-go` (official driver)
  - Apache Druid: `github.com/apache/druid-go-client` or REST API client
  - HDFS/Kerberos: `github.com/colinmarc/hdfs/v2` + `github.com/jcmturner/gokrb5`
  - Domestic databases: Vendor-specific Go drivers where available, MySQL/PostgreSQL protocol compatibility where applicable

**Storage**: MySQL (existing - stores data source configurations), extended schema for new source types
**Testing**: `testing` package + `testify` for assertions (existing), new integration tests for each data source type
**Target Platform**: Linux server (existing), containerized deployment (Docker/Kubernetes)
**Project Type**: Web application (REST API gateway)
**Performance Goals**:
- P95 query latency < 30s for data lakes, < 60s for warehouses, < 5s for OLAP
- Gateway overhead < 100ms for routing and validation
- Support 50+ concurrent query connections
- Stream result sets up to 10M rows without memory overflow (512MB per query limit)
**Constraints**:
- Read-only query enforcement (SELECT only) across all source types
- Authentication token auto-rotation before expiration
- Chinese character encoding support for domestic databases
- Schema evolution handling without query failures
- Compatibility with existing driver registry pattern
**Scale/Scope**:
- 6 categories of data sources
- 30+ specific driver implementations (including domestic databases)
- 55 functional requirements
- API versioning via `/api/v1/` prefix (existing pattern)

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

### I. Gateway-First Architecture ✅ PASS
**Requirement**: Every feature designed with gateway patterns (request routing, protocol translation, service aggregation).

**Compliance**: Phase 1 extends existing gateway architecture with new data source drivers. The gateway remains the single entry point, handling authentication, rate limiting, and query validation before routing to backend data sources. New drivers integrate via existing `DriverRegistry` pattern in `internal/database/drivers.go`. Query execution flows through existing `QueryService` → `ConnectionPool` → `Driver` chain.

### II. API Contract Management ✅ PASS (Phase 1 Complete)
**Requirement**: All API contracts defined before implementation using OpenAPI/Swagger. Versioned. Breaking changes require MAJOR version increment.

**Compliance**: Existing API endpoints (`POST /api/v1/query`, `POST /api/v1/datasources`) will be extended, not broken. New data source types are additive to existing `DataSource` model enum. New request/response DTOs will be versioned in `/contracts/` directory.

**Phase 1 Deliverable**: ✅ OpenAPI 3.0 spec generated at `contracts/openapi.yaml` with all Phase 1 endpoints documented. No breaking changes to existing `/api/v1/` endpoints.

### III. Test-First Development ✅ PASS
**Requirement**: TDD mandatory. Tests written → User approved → Tests fail → Then implement.

**Compliance**: Research phase (Phase 0) identified testing patterns for each data source category. Phase 1 design includes test strategy: unit tests for each driver, integration tests with testcontainers, contract tests validating OpenAPI spec. Implementation will follow TDD: tests written first, then implementation.

### IV. Observability & Monitoring ✅ PASS
**Requirement**: Structured logging, correlation IDs, metrics (p50/p95/p99 latency, error rates), distributed tracing, health checks.

**Compliance**: Existing `internal/middleware/correlation.go` provides correlation IDs. Phase 1 design includes query execution metrics (FR-045): execution time, rows scanned, data processed volume. Health checks extended to all 30+ data source types (FR-051). Metrics exported via `/metrics` endpoint for Prometheus.

### V. Security & Rate Limiting ✅ PASS
**Requirement**: Auth enforced at gateway layer. Rate limiting per client/API key. TLS encryption. Secure credential storage.

**Compliance**: Existing JWT authentication and rate limiting (`internal/middleware/rate_limit.go`) apply to all new data sources. Phase 1 data model includes `CredentialVault` entity for encrypted credential storage (FR-035). IAM-based auth for AWS (FR-036), OAuth2 for Google Cloud (FR-037), Kerberos for HDFS (FR-038). Read-only enforcement (FR-041) prevents data modification. Token auto-rotation (FR-039) designed in connection pool layer.

### VI. Versioning & Backward Compatibility ✅ PASS
**Requirement**: Semantic versioning. MAJOR for breaking changes, MINOR for additive features. Deprecated endpoints supported 6+ months.

**Compliance**: Phase 1 is additive - extends `DatabaseType` enum with 30+ new values, adds driver implementations. No existing endpoints removed. New fields added to `DataSourceConfig` JSON (backward compatible via `AdditionalProps` map). API remains `/api/v1/`. No breaking changes to existing contract.

### Final Gate Decision: **✅ PASS - READY FOR IMPLEMENTATION**
✅ All constitution principles satisfied
✅ OpenAPI spec generated in Phase 1 (`contracts/openapi.yaml`)
✅ Data model designed with schema evolution support
✅ Security patterns defined (encrypted credentials, multi-protocol auth)
✅ Observability integrated (metrics, structured logging, correlation IDs)
✅ No breaking changes to existing API

## Project Structure

### Documentation (this feature)

```text
specs/001-phase1-enhanced-single-source/
├── plan.md              # This file (/speckit.plan command output)
├── research.md          # Phase 0 output (/speckit.plan command)
├── data-model.md        # Phase 1 output (/speckit.plan command)
├── quickstart.md        # Phase 1 output (/speckit.plan command)
├── contracts/           # Phase 1 output (/speckit.plan command)
│   ├── openapi.yaml     # OpenAPI 3.0 spec for all API endpoints
│   └── datasource-schemas.json  # JSON schemas for DataSourceConfig variations
└── tasks.md             # Phase 2 output (/speckit.tasks command - NOT created by /speckit.plan)
```

### Source Code (repository root)

```text
# Existing structure - Option 2: Web application
backend/ (root directory)
├── cmd/
│   └── server/
│       └── main.go              # Existing entry point
├── internal/
│   ├── model/
│   │   ├── datasource.go        # Extended with new DatabaseType enums
│   │   └── query.go             # Existing query models
│   ├── database/
│   │   ├── drivers.go           # Extended DriverRegistry with 30+ drivers
│   │   ├── connection_pool.go   # Enhanced with token rotation
│   │   ├── health_checker.go    # Extended health checks
│   │   ├── drivers/             # NEW: Driver implementations organized by category
│   │   │   ├── table_formats/   # Iceberg, Delta Lake, Hudi drivers
│   │   │   ├── warehouses/      # Snowflake, Databricks, Redshift, BigQuery
│   │   │   ├── object_storage/  # S3, MinIO, OSS, COS, Azure Blob
│   │   │   ├── olap/           # ClickHouse, Doris, StarRocks, Druid
│   │   │   ├── domestic/       # OceanBase, TiDB, TDSQL, GaussDB, DaMeng, etc.
│   │   │   └── filesystems/    # HDFS, Apache Ozone
│   │   └── metadata/            # NEW: Schema metadata extraction
│   │       └── schema_cache.go  # Cache table schemas for query validation
│   ├── security/
│   │   ├── sql_validator.go     # Enhanced with source-specific validation
│   │   ├── auth_middleware.go   # Existing JWT auth
│   │   ├── credential_vault.go  # NEW: Encrypted credential storage
│   │   └── token_manager.go     # NEW: Auto-rotation of auth tokens
│   ├── service/
│   │   ├── datasource_service.go # Extended with new source types
│   │   └── query_service.go      # Enhanced query routing logic
│   ├── controller/
│   │   ├── datasource_controller.go  # Extended with new type support
│   │   └── query_controller.go       # Existing query endpoint
│   └── middleware/
│       ├── correlation.go       # Existing
│       ├── rate_limit.go        # Existing
│       └── cors.go              # Existing
├── tests/
│   ├── contract/                # NEW: API contract tests
│   │   ├── datasources_test.go  # Validate OpenAPI spec compliance
│   │   └── query_test.go        # Validate query endpoint contract
│   ├── integration/             # NEW: End-to-end tests for each driver
│   │   ├── iceberg_test.go      # Test Iceberg table queries
│   │   ├── delta_test.go        # Test Delta Lake queries
│   │   ├── snowflake_test.go    # Test Snowflake connectivity
│   │   ├── clickhouse_test.go   # Test ClickHouse queries
│   │   └── ...
│   └── unit/
│       ├── drivers_test.go      # Unit tests for driver logic
│       └── metadata_test.go     # Schema extraction tests
├── configs/
│   └── config.yaml              # Extended with new driver configs
├── deployments/
│   └── docker/
│       └── Dockerfile           # Extended with new dependencies
└── go.mod                       # Updated with new dependencies
```

**Structure Decision**: Web application structure (existing) maintained. Phase 1 is additive - extends existing layers without architectural changes. New driver implementations organized under `internal/database/drivers/` by category for maintainability. No separate frontend/mobile components (gateway is API-only).

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| N/A | N/A | No constitution violations - all principles satisfied |

**Note**: 30+ driver implementations may seem complex, but each follows the existing `Driver` interface pattern. Complexity is managed through:
1. Consistent interface abstraction (existing `Driver` interface)
2. Category-based organization (table_formats/, warehouses/, etc.)
3. Shared utilities (connection pooling, auth, metadata)
4. Incremental implementation (P1-P6 prioritization)
