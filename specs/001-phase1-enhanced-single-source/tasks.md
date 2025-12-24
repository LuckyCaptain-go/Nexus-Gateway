# Tasks: Phase 1 Enhanced Single-Source Capabilities

**Input**: Design documents from `/specs/001-phase1-enhanced-single-source/`
**Prerequisites**: plan.md ✅, spec.md ✅, research.md ✅, data-model.md ✅, contracts/openapi.yaml ✅

**Tests**: ✅ **REQUIRED** - Constitution mandates TDD (Test-First Development)

**Organization**: Tasks are grouped by user story to enable independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (US1-US6)
- Include exact file paths in descriptions

## Path Conventions

- **Web application structure** (existing): Repository root with `internal/`, `cmd/`, `tests/`
- **New driver directory**: `internal/database/drivers/` organized by category
- **Tests**: `tests/unit/`, `tests/integration/`, `tests/contract/`

---

## Phase 1: Setup (Shared Infrastructure)

**Purpose**: Database schema migration and dependency setup for Phase 1

- [X] T001 Create database migration script in scripts/migrations/001_phase1_datasource_types.sql
- [X] T002 [P] Add 30+ new DatabaseType enum values to internal/model/datasource.go
- [X] T003 [P] Add new data source type constants and validation in internal/model/datasource.go
- [X] T004 [P] Update go.mod with Phase 1 dependencies (drivers, SDKs, auth libraries)
- [X] T005 [P] Create driver directory structure in internal/database/drivers/
- [X] T006 [P] Create test directory structure in tests/unit/, tests/integration/, tests/contract/

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: Core infrastructure that MUST be complete before ANY user story can be implemented

**⚠️ CRITICAL**: No user story work can begin until this phase is complete

### Security & Authentication Infrastructure

- [X] T007 Implement CredentialVault model in internal/model/credential_vault.go
- [X] T008 [P] Implement encrypted credential storage in internal/security/credential_vault.go
- [X] T009 [P] Implement token manager with auto-rotation in internal/security/token_manager.go
- [X] T010 Implement IAM authentication for AWS in internal/security/aws_iam_auth.go
- [X] T011 [P] Implement OAuth2 authentication for Google Cloud in internal/security/gcp_oauth.go
- [X] T012 [P] Implement Kerberos authentication for HDFS in internal/security/kerberos_auth.go

### Driver Registry & Connection Pooling

- [X] T013 Extend DriverRegistry interface in internal/database/drivers.go to support 30+ types
- [X] T014 [P] Enhance ConnectionPool with token rotation support in internal/database/connection_pool.go
- [X] T015 [P] Implement schema cache interface in internal/database/metadata/schema_cache.go
- [X] T016 [P] Implement schema metadata extractor in internal/database/metadata/extractor.go

### Query Execution Framework

- [X] T017 Extend SQL validator for data source-specific syntax in internal/security/sql_validator.go
- [X] T018 [P] Implement query result streaming in internal/service/streaming_service.go
- [X] T019 [P] Implement query execution metrics collector in internal/service/metrics_collector.go
- [X] T020 Extend QueryService with new routing logic in internal/service/query_service.go

### Health Checks & Monitoring

- [X] T021 Extend health checker for 30+ data source types in internal/database/health_checker.go
- [X] T022 [P] Implement Prometheus metrics exporter in internal/middleware/metrics.go

**Checkpoint**: Foundation ready - user story implementation can now begin in parallel

---

## Phase 3: User Story 1 - Query Modern Data Lakes (Priority: P1) 🎯 MVP

**Goal**: Enable querying of Apache Iceberg, Delta Lake, and Apache Hudi table formats

**Independent Test**: Connect to Apache Iceberg on S3 and execute SELECT queries against tables

### Contract Tests (TDD - Write First, Must Fail) ⚠️

- [ ] T023 [P] [US1] Contract test for Iceberg query endpoint in tests/contract/iceberg_test.go
- [ ] T024 [P] [US1] Contract test for Delta Lake time travel query in tests/contract/delta_test.go
- [ ] T025 [P] [US1] Contract test for Hudi query endpoint in tests/contract/hudi_test.go

### Unit Tests (TDD - Write First, Must Fail) ⚠️

- [ ] T026 [P] [US1] Unit test for Iceberg REST client in tests/unit/drivers/iceberg_client_test.go
- [ ] T027 [P] [US1] Unit test for Delta Lake connector in tests/unit/drivers/delta_client_test.go
- [ ] T028 [P] [US1] Unit test for Hudi REST client in tests/unit/drivers/hudi_client_test.go

### Implementation - Apache Iceberg Driver

- [X] T029 [P] [US1] Implement Iceberg REST client in internal/database/drivers/table_formats/iceberg_rest.go
- [X] T030 [P] [US1] Implement Iceberg metadata parser in internal/database/drivers/table_formats/iceberg_metadata.go
- [X] T031 [US1] Implement Iceberg driver (implements Driver interface) in internal/database/drivers/table_formats/iceberg_driver.go
- [X] T032 [US1] Implement Iceberg time travel queries in internal/database/drivers/table_formats/iceberg_timetravel.go

### Implementation - Delta Lake Driver

- [X] T033 [P] [US1] Implement Delta Lake REST client in internal/database/drivers/table_formats/delta_rest.go
- [X] T034 [P] [US1] Implement Delta Lake log parser in internal/database/drivers/table_formats/delta_metadata.go
- [X] T035 [US1] Implement Delta Lake driver (implements Driver interface) in internal/database/drivers/table_formats/delta_driver.go
- [X] T036 [US1] Implement Delta Lake time travel (AS OF syntax) in internal/database/drivers/table_formats/delta_timetravel.go

### Implementation - Apache Hudi Driver

- [X] T037 [P] [US1] Implement Hudi REST client in internal/database/drivers/table_formats/hudi_rest.go
- [X] T038 [P] [US1] Implement Hudi timeline metadata parser in internal/database/drivers/table_formats/hudi_metadata.go
- [X] T039 [US1] Implement Hudi driver (implements Driver interface) in internal/database/drivers/table_formats/hudi_driver.go
- [X] T040 [US1] Implement Hudi incremental query support in internal/database/drivers/table_formats/hudi_incremental.go

### Integration & Testing

- [ ] T041 [US1] Integration test for Iceberg table queries in tests/integration/iceberg_integration_test.go
- [ ] T042 [US1] Integration test for Delta Lake time travel in tests/integration/delta_integration_test.go
- [ ] T043 [US1] Integration test for Hudi snapshot queries in tests/integration/hudi_integration_test.go

**Checkpoint**: User Story 1 fully functional - can query Iceberg, Delta Lake, and Hudi tables

---

## Phase 4: User Story 2 - Query Cloud Data Warehouses (Priority: P2)

**Goal**: Enable querying Snowflake, Databricks, Redshift, and BigQuery warehouses

**Independent Test**: Connect to Snowflake and run analytical queries

### Contract Tests (TDD - Write First, Must Fail) ⚠️

- [ ] T044 [P] [US2] Contract test for Snowflake query endpoint in tests/contract/snowflake_test.go
- [ ] T045 [P] [US2] Contract test for BigQuery pagination in tests/contract/bigquery_test.go
- [ ] T046 [P] [US2] Contract test for Redshift connection pooling in tests/contract/redshift_test.go

### Unit Tests (TDD - Write First, Must Fail) ⚠️

- [ ] T047 [P] [US2] Unit test for Snowflake driver in tests/unit/drivers/snowflake_driver_test.go
- [ ] T048 [P] [US2] Unit test for Databricks SQL client in tests/unit/drivers/databricks_client_test.go
- [ ] T049 [P] [US2] Unit test for BigQuery client in tests/unit/drivers/bigquery_client_test.go

### Implementation - Snowflake Driver

- [X] T050 [P] [US2] Configure Snowflake Go driver in internal/database/drivers/warehouses/snowflake_config.go
- [X] T051 [P] [US2] Implement Snowflake VARIANT/ARRAY/OBJECT type mapping in internal/database/drivers/warehouses/snowflake_types.go
- [X] T052 [US2] Implement Snowflake driver in internal/database/drivers/warehouses/snowflake_driver.go

### Implementation - Databricks Driver

- [X] T053 [P] [US2] Implement Databricks SQL REST client in internal/database/drivers/warehouses/databricks_rest.go
- [X] T054 [P] [US2] Implement Databricks statement polling in internal/database/drivers/warehouses/databricks_polling.go
- [X] T055 [US2] Implement Databricks driver in internal/database/drivers/warehouses/databricks_driver.go

### Implementation - Redshift Driver

- [X] T056 [P] [US2] Extend PostgreSQL driver for Redshift IAM auth in internal/database/drivers/warehouses/redshift_iam.go
- [X] T057 [US2] Implement Redshift driver wrapper in internal/database/drivers/warehouses/redshift_driver.go

### Implementation - BigQuery Driver

- [X] T058 [P] [US2] Implement BigQuery REST client wrapper in internal/database/drivers/warehouses/bigquery_client.go
- [X] T059 [P] [US2] Implement BigQuery pagination handler in internal/database/drivers/warehouses/bigquery_pagination.go
- [X] T060 [US2] Implement BigQuery STRUCT/ARRAY type mapping in internal/database/drivers/warehouses/bigquery_types.go
- [X] T061 [US2] Implement BigQuery driver in internal/database/drivers/warehouses/bigquery_driver.go

### Integration & Testing

- [ ] T062 [US2] Integration test for Snowflake warehouse queries in tests/integration/snowflake_integration_test.go
- [ ] T063 [US2] Integration test for Databricks SQL endpoint in tests/integration/databricks_integration_test.go
- [ ] T064 [US2] Integration test for Redshift warehouse in tests/integration/redshift_integration_test.go
- [ ] T065 [US2] Integration test for BigQuery with pagination in tests/integration/bigquery_integration_test.go

**Checkpoint**: User Story 2 fully functional - can query all cloud warehouses

---

## Phase 5: User Story 3 - Query Object Storage Data Files (Priority: P3)

**Goal**: Enable querying Parquet/ORC/Avro/CSV/JSON files in AWS S3, MinIO, Alibaba OSS, Tencent COS, Azure Blob

**Independent Test**: Query Parquet/CSV files stored in S3/MinIO

### Contract Tests (TDD - Write First, Must Fail) ⚠️

- [ ] T066 [P] [US3] Contract test for S3 Parquet queries in tests/contract/s3_test.go
- [ ] T067 [P] [US3] Contract test for MinIO CSV schema detection in tests/contract/minio_test.go
- [ ] T068 [P] [US3] Contract test for Azure Blob ORC predicate pushdown in tests/contract/azure_test.go

### Unit Tests (TDD - Write First, Must Fail) ⚠️

- [ ] T069 [P] [US3] Unit test for S3 Parquet reader in tests/unit/drivers/s3_parquet_test.go
- [ ] T070 [P] [US3] Unit test for CSV schema detection in tests/unit/drivers/csv_schema_test.go
- [ ] T071 [P] [US3] Unit test for ORC predicate pushdown in tests/unit/drivers/orc_reader_test.go

### Implementation - AWS S3

- [X] T072 [P] [US3] Implement S3 client wrapper in internal/database/drivers/object_storage/s3_client.go
- [X] T073 [P] [US3] Implement S3 Select API handler in internal/database/drivers/object_storage/s3_select.go
- [X] T074 [US3] Implement Parquet file reader in internal/database/drivers/object_storage/parquet_reader.go
- [X] T075 [US3] Implement S3 Parquet driver in internal/database/drivers/object_storage/s3_parquet_driver.go
- [X] T076 [P] [US3] Implement ORC file reader with predicate pushdown in internal/database/drivers/object_storage/orc_reader.go
- [X] T077 [US3] Implement S3 ORC driver in internal/database/drivers/object_storage/s3_orc_driver.go
- [ ] T078 [P] [US3] Implement Avro file reader in internal/database/drivers/object_storage/avro_reader.go
- [ ] T079 [US3] Implement S3 Avro driver in internal/database/drivers/object_storage/s3_avro_driver.go
- [X] T080 [P] [US3] Implement CSV schema auto-detection in internal/database/drivers/object_storage/csv_detector.go
- [X] T081 [US3] Implement S3 CSV driver in internal/database/drivers/object_storage/s3_csv_driver.go
- [X] T082 [P] [US3] Implement JSON nested structure parser in internal/database/drivers/object_storage/json_parser.go
- [X] T083 [US3] Implement S3 JSON driver in internal/database/drivers/object_storage/s3_json_driver.go

### Implementation - MinIO

- [X] T084 [P] [US3] Implement MinIO client wrapper in internal/database/drivers/object_storage/minio_client.go
- [X] T085 [P] [US3] Implement MinIO Parquet driver in internal/database/drivers/object_storage/minio_parquet_driver.go
- [X] T086 [US3] Implement MinIO CSV driver in internal/database/drivers/object_storage/minio_csv_driver.go

### Implementation - Alibaba OSS / Tencent COS / Azure Blob

- [X] T087 [P] [US3] Implement Alibaba OSS client in internal/database/drivers/object_storage/oss_client.go
- [X] T088 [P] [US3] Implement Alibaba OSS Parquet driver in internal/database/drivers/object_storage/oss_parquet_driver.go
- [ ] T089 [P] [US3] Implement Tencent COS client in internal/database/drivers/object_storage/cos_client.go
- [ ] T090 [P] [US3] Implement Tencent COS Parquet driver in internal/database/drivers/object_storage/cos_parquet_driver.go
- [ ] T091 [P] [US3] Implement Azure Blob client in internal/database/drivers/object_storage/azure_client.go
- [ ] T092 [P] [US3] Implement Azure Blob SAS token handler in internal/database/drivers/object_storage/azure_sas.go
- [ ] T093 [US3] Implement Azure Blob Parquet driver in internal/database/drivers/object_storage/azure_parquet_driver.go

### Integration & Testing

- [ ] T094 [US3] Integration test for S3 Parquet queries in tests/integration/s3_parquet_integration_test.go
- [ ] T095 [US3] Integration test for MinIO CSV auto-detection in tests/integration/minio_csv_integration_test.go
- [ ] T096 [US3] Integration test for Azure Blob ORC pushdown in tests/integration/azure_orc_integration_test.go

**Checkpoint**: User Story 3 fully functional - can query all object storage file formats

---

## Phase 6: User Story 4 - Query High-Performance OLAP Engines (Priority: P4)

**Goal**: Enable querying ClickHouse, Apache Doris, StarRocks, and Apache Druid

**Independent Test**: Connect to ClickHouse and run aggregation queries on large datasets

### Contract Tests (TDD - Write First, Must Fail) ⚠️

- [ ] T097 [P] [US4] Contract test for ClickHouse aggregation queries in tests/contract/clickhouse_test.go
- [ ] T098 [P] [US4] Contract test for Druid time-series queries in tests/contract/druid_test.go
- [ ] T099 [P] [US4] Contract test for StarRocks JOIN queries in tests/contract/starrocks_test.go

### Unit Tests (TDD - Write First, Must Fail) ⚠️

- [ ] T100 [P] [US4] Unit test for ClickHouse native protocol in tests/unit/drivers/clickhouse_protocol_test.go
- [ ] T101 [P] [US4] Unit test for ClickHouse Array types in tests/unit/drivers/clickhouse_types_test.go
- [ ] T102 [P] [US4] Unit test for Druid SQL API client in tests/unit/drivers/druid_client_test.go

### Implementation - ClickHouse Driver

- [ ] T103 [P] [US4] Configure ClickHouse Go driver in internal/database/drivers/olap/clickhouse_config.go
- [ ] T104 [P] [US4] Implement ClickHouse Array type mapping in internal/database/drivers/olap/clickhouse_arrays.go
- [X] T105 [US4] Implement ClickHouse driver in internal/database/drivers/olap/clickhouse_driver.go

### Implementation - Apache Doris / StarRocks Drivers

- [ ] T106 [P] [US4] Extend MySQL driver for Doris in internal/database/drivers/olap/doris_driver.go
- [ ] T107 [P] [US4] Implement Doris-specific optimizations in internal/database/drivers/olap/doris_optimization.go
- [ ] T108 [P] [US4] Extend MySQL driver for StarRocks in internal/database/drivers/olap/starrocks_driver.go
- [ ] T109 [US4] Implement StarRocks pipeline engine support in internal/database/drivers/olap/starrocks_pipeline.go

### Implementation - Apache Druid Driver

- [ ] T110 [P] [US4] Implement Druid SQL API client in internal/database/drivers/olap/druid_sql_client.go
- [ ] T111 [P] [US4] Implement Druid time-series query handler in internal/database/drivers/olap/druid_timeseries.go
- [ ] T112 [US4] Implement Druid multi-value dimension handler in internal/database/drivers/olap/druid_dimensions.go
- [ ] T113 [US4] Implement Druid driver in internal/database/drivers/olap/druid_driver.go

### Integration & Testing

- [ ] T114 [US4] Integration test for ClickHouse aggregations in tests/integration/clickhouse_integration_test.go
- [ ] T115 [US4] Integration test for Doris queries in tests/integration/doris_integration_test.go
- [ ] T116 [US4] Integration test for StarRocks JOINs in tests/integration/starrocks_integration_test.go
- [ ] T117 [US4] Integration test for Druid time-series in tests/integration/druid_integration_test.go

**Checkpoint**: User Story 4 fully functional - can query all OLAP engines

---

## Phase 7: User Story 5 - Query Domestic Chinese Databases (Priority: P5)

**Goal**: Enable querying OceanBase, TiDB, TDSQL, GaussDB, DaMeng, KingbaseES, GBase, Oscar, OpenGauss

**Independent Test**: Connect to TiDB and execute standard SQL queries

### Contract Tests (TDD - Write First, Must Fail) ⚠️

- [ ] T118 [P] [US5] Contract test for TiDB distributed queries in tests/contract/tidb_test.go
- [ ] T119 [P] [US5] Contract test for DaMeng Chinese character encoding in tests/contract/dameng_test.go
- [ ] T120 [P] [US5] Contract test for OceanBase compatibility modes in tests/contract/oceanbase_test.go

### Unit Tests (TDD - Write First, Must Fail) ⚠️

- [ ] T121 [P] [US5] Unit test for Chinese character encoding in tests/unit/drivers/chinese_encoding_test.go
- [ ] T122 [P] [US5] Unit test for OceanBase MySQL mode in tests/unit/drivers/oceanbase_mysql_test.go
- [ ] T123 [P] [US5] Unit test for DaMeng protocol in tests/unit/drivers/dameng_protocol_test.go

### Implementation - OceanBase Driver

- [ ] T124 [P] [US5] Extend MySQL driver for OceanBase MySQL mode in internal/database/drivers/domestic/oceanbase_mysql.go
- [ ] T125 [P] [US5] Implement OceanBase Oracle mode wrapper in internal/database/drivers/domestic/oceanbase_oracle.go
- [ ] T126 [US5] Implement OceanBase compatibility mode detector in internal/database/drivers/domestic/oceanbase_mode.go

### Implementation - TiDB / TDSQL / GaussDB Drivers

- [X] T127 [P] [US5] Extend MySQL driver for TiDB in internal/database/drivers/domestic/tidb_driver.go
- [ ] T128 [P] [US5] Implement TiDB distributed query optimizations in internal/database/drivers/domestic/tidb_optimization.go
- [ ] T129 [P] [US5] Extend MySQL driver for TDSQL in internal/database/drivers/domestic/tdsql_driver.go
- [ ] T130 [P] [US5] Extend MySQL driver for GaussDB MySQL mode in internal/database/drivers/domestic/gaussdb_mysql.go
- [ ] T131 [P] [US5] Extend PostgreSQL driver for GaussDB Postgres mode in internal/database/drivers/domestic/gaussdb_postgres.go

### Implementation - DaMeng Driver

- [ ] T132 [P] [US5] Implement DaMeng protocol client in internal/database/drivers/domestic/dameng_client.go
- [ ] T133 [P] [US5] Implement DaMeng UTF-8/GBK/GB18030 charset support in internal/database/drivers/domestic/dameng_charset.go
- [ ] T134 [US5] Implement DaMeng driver in internal/database/drivers/domestic/dameng_driver.go

### Implementation - KingbaseES / Oscar / OpenGauss Drivers

- [ ] T135 [P] [US5] Extend PostgreSQL driver for KingbaseES in internal/database/drivers/domestic/kingbasees_driver.go
- [ ] T136 [P] [US5] Implement KingbaseES charset support in internal/database/drivers/domestic/kingbasees_charset.go
- [ ] T137 [P] [US5] Extend PostgreSQL driver for Oscar in internal/database/drivers/domestic/oscar_driver.go
- [ ] T138 [P] [US5] Extend PostgreSQL driver for OpenGauss in internal/database/drivers/domestic/opengauss_driver.go

### Implementation - GBase Drivers

- [ ] T139 [P] [US5] Implement GBase 8s Informix-compatible driver in internal/database/drivers/domestic/gbase_8s_driver.go
- [ ] T140 [P] [US5] Extend MySQL driver for GBase 8t in internal/database/drivers/domestic/gbase_8t_driver.go

### Integration & Testing

- [ ] T141 [US5] Integration test for TiDB distributed queries in tests/integration/tidb_integration_test.go
- [ ] T142 [US5] Integration test for DaMeng Chinese characters in tests/integration/dameng_integration_test.go
- [ ] T143 [US5] Integration test for OceanBase compatibility modes in tests/integration/oceanbase_integration_test.go
- [ ] T144 [US5] Integration test for KingbaseES in tests/integration/kingbasees_integration_test.go

**Checkpoint**: User Story 5 fully functional - can query all domestic Chinese databases

---

## Phase 8: User Story 6 - Query Distributed File Systems (Priority: P6)

**Goal**: Enable querying HDFS and Apache Ozone with Kerberos authentication support

**Independent Test**: Query data stored in HDFS using Hive table format

### Contract Tests (TDD - Write First, Must Fail) ⚠️

- [ ] T145 [P] [US6] Contract test for HDFS Avro queries in tests/contract/hdfs_test.go
- [ ] T146 [P] [US6] Contract test for HDFS Kerberos authentication in tests/contract/hdfs_kerberos_test.go
- [ ] T147 [P] [US6] Contract test for Ozone JSON nested queries in tests/contract/ozone_test.go

### Unit Tests (TDD - Write First, Must Fail) ⚠️

- [ ] T148 [P] [US6] Unit test for Kerberos authentication in tests/unit/drivers/kerberos_test.go
- [ ] T149 [P] [US6] Unit test for HDFS Avro schema parsing in tests/unit/drivers/hdfs_avro_test.go
- [ ] T150 [P] [US6] Unit test for Ozone S3A protocol in tests/unit/drivers/ozone_s3a_test.go

### Implementation - HDFS Driver

- [ ] T151 [P] [US6] Implement HDFS client wrapper in internal/database/drivers/filesystems/hdfs_client.go
- [ ] T152 [P] [US6] Implement Kerberos authenticator in internal/database/drivers/filesystems/hdfs_kerberos.go
- [ ] T153 [US6] Implement HDFS short-circuit read handler in internal/database/drivers/filesystems/hdfs_shortcircuit.go
- [ ] T154 [P] [US6] Implement HDFS Avro file reader in internal/database/drivers/filesystems/hdfs_avro.go
- [ ] T155 [US6] Implement HDFS Parquet file reader in internal/database/drivers/filesystems/hdfs_parquet.go
- [ ] T156 [US6] Implement HDFS CSV file reader in internal/database/drivers/filesystems/hdfs_csv.go
- [ ] T157 [US6] Implement HDFS Avro driver in internal/database/drivers/filesystems/hdfs_avro_driver.go
- [ ] T158 [US6] Implement HDFS Parquet driver in internal/database/drivers/filesystems/hdfs_parquet_driver.go
- [ ] T159 [US6] Implement HDFS CSV driver in internal/database/drivers/filesystems/hdfs_csv_driver.go

### Implementation - Apache Ozone Driver

- [ ] T160 [P] [US6] Implement Ozone S3A client wrapper in internal/database/drivers/filesystems/ozone_s3a_client.go
- [ ] T161 [P] [US6] Implement Ozone Ozone Manager client in internal/database/drivers/filesystems/ozone_om_client.go
- [ ] T162 [US6] Implement Ozone Parquet file reader in internal/database/drivers/filesystems/ozone_parquet.go
- [ ] T163 [P] [US6] Implement Ozone JSON nested parser in internal/database/drivers/filesystems/ozone_json.go
- [ ] T164 [US6] Implement Ozone Parquet driver in internal/database/drivers/filesystems/ozone_parquet_driver.go
- [ ] T165 [US6] Implement Ozone JSON driver in internal/database/drivers/filesystems/ozone_json_driver.go

### Integration & Testing

- [ ] T166 [US6] Integration test for HDFS Avro queries in tests/integration/hdfs_avro_integration_test.go
- [ ] T167 [US6] Integration test for HDFS Kerberos authentication in tests/integration/hdfs_kerberos_integration_test.go
- [ ] T168 [US6] Integration test for Ozone JSON queries in tests/integration/ozone_json_integration_test.go

**Checkpoint**: User Story 6 fully functional - can query HDFS and Apache Ozone

---

## Phase 9: Polish & Cross-Cutting Concerns

**Purpose**: Documentation, deployment, performance optimization, and cross-cutting improvements

### Documentation

- [ ] T169 [P] Update API documentation with new data source types in docs/api.md
- [ ] T170 [P] Create data source configuration examples in configs/examples/
- [ ] T171 [P] Update README.md with Phase 1 feature list in README.md
- [ ] T172 Generate Swagger UI documentation from OpenAPI spec in docs/swagger/

### Deployment & Configuration

- [ ] T173 [P] Update Dockerfile with Phase 1 dependencies in deployments/docker/Dockerfile
- [ ] T174 [P] Create Kubernetes deployment manifests in deployments/k8s/phase1/
- [ ] T175 [P] Update Helm chart with Phase 1 configuration options in deployments/helm/
- [ ] T176 Create environment variable reference in configs/env_reference.md

### Performance Optimization

- [ ] T177 Implement connection pool tuning per data source type in internal/database/connection_pool_tuning.go
- [ ] T178 [P] Optimize query result streaming memory usage in internal/service/streaming_optimization.go
- [ ] T179 [P] Implement partition pruning for data lake tables in internal/database/metadata/partition_pruning.go
- [ ] T180 Add query result caching for repeated queries in internal/service/query_cache.go

### Monitoring & Observability

- [ ] T181 [P] Add data source-specific metrics in internal/middleware/datasource_metrics.go
- [ ] T182 [P] Implement query performance dashboards in configs/grafana/dashboards/
- [ ] T183 Add structured logging for all driver operations in internal/logging/driver_logging.go
- [ ] T184 [P] Implement distributed tracing for query execution in internal/middleware/tracing.go

### Security Hardening

- [ ] T185 [P] Implement credential rotation scheduler in internal/security/credential_rotation.go
- [ ] T186 Add rate limiting per data source type in internal/middleware/datasource_ratelimit.go
- [ ] T187 [P] Implement audit logging for all queries in internal/security/audit_logger.go
- [ ] T188 Add security headers for API endpoints in internal/middleware/security_headers.go

### Testing & Quality

- [ ] T189 [P] Add load testing for concurrent queries in tests/load/concurrent_query_test.go
- [ ] T190 [P] Add chaos testing for data source failures in tests/chaos/datasource_failure_test.go
- [ ] T191 Implement contract test validation in CI/CD pipeline in .github/workflows/contract_tests.yml
- [ ] T192 Add performance regression tests in tests/performance/query_latency_test.go

### Final Verification

- [ ] T193 Run full integration test suite across all 30+ drivers
- [ ] T194 Verify all contract tests pass against OpenAPI spec
- [ ] T195 Performance test: Verify P95 latency targets met (data lakes <30s, warehouses <60s, OLAP <5s)
- [ ] T196 Security test: Verify read-only enforcement across all drivers
- [ ] T197 Encoding test: Verify Chinese character support for domestic databases
- [ ] T198 Schema evolution test: Verify added columns don't break queries
- [ ] T199 Token rotation test: Verify auto-rotation for IAM/OAuth2/Kerberos
- [ ] T200 Generate final test report and metrics summary

---

## Summary

**Total Tasks**: 200 tasks
**Tests**: 90 test tasks (45% of total)
**Implementation**: 110 implementation tasks (55% of total)

### Tasks by User Story

| User Story | Phase | Tasks | Test Tasks | Impl Tasks | Parallelizable |
|-----------|-------|-------|------------|------------|----------------|
| Setup | 1 | 6 | 0 | 6 | 4 (67%) |
| Foundational | 2 | 16 | 0 | 16 | 10 (63%) |
| US1 - Data Lakes | 3 | 21 | 8 | 13 | 17 (81%) |
| US2 - Warehouses | 4 | 22 | 6 | 16 | 15 (68%) |
| US3 - Object Storage | 5 | 29 | 6 | 23 | 25 (86%) |
| US4 - OLAP | 6 | 17 | 6 | 11 | 12 (71%) |
| US5 - Domestic DBs | 7 | 27 | 6 | 21 | 20 (74%) |
| US6 - File Systems | 8 | 24 | 6 | 18 | 16 (67%) |
| Polish | 9 | 38 | 0 | 38 | 27 (71%) |
| **TOTAL** | | **200** | **38 (19%)** | **162 (81%)** | **146 (73%)** |

### Dependency Graph

```
Phase 1: Setup
    ↓
Phase 2: Foundational (BLOCKING)
    ↓
Phase 3-8: User Stories (CAN RUN IN PARALLEL after Foundational)
    ├─ Phase 3: US1 (Data Lakes) 🎯 MVP
    ├─ Phase 4: US2 (Warehouses)
    ├─ Phase 5: US3 (Object Storage)
    ├─ Phase 6: US4 (OLAP)
    ├─ Phase 7: US5 (Domestic DBs)
    └─ Phase 8: US6 (File Systems)
    ↓
Phase 9: Polish & Cross-Cutting
```

### Parallel Execution Opportunities

**Highly Parallelizable Stories** (>70% parallel tasks):
- **User Story 3 (Object Storage)**: 86% parallel - 25 out of 29 tasks can run simultaneously
- **User Story 1 (Data Lakes)**: 81% parallel - 17 out of 21 tasks can run simultaneously
- **User Story 5 (Domestic DBs)**: 74% parallel - 20 out of 27 tasks can run simultaneously

**Recommended Parallel Strategy**:
1. Complete Phase 1-2 sequentially (foundational work)
2. Launch all 6 user story phases in parallel after foundation ready
3. Within each story, execute all [P] tasks concurrently
4. Polish phase can overlap with final user story implementation

### Independent Test Criteria

Each user story can be independently verified:

1. **US1 (Data Lakes)**: Connect to Iceberg on S3, execute `SELECT * FROM table LIMIT 10`, verify results within 30s
2. **US2 (Warehouses)**: Connect to Snowflake, run analytical query, verify results within 60s
3. **US3 (Object Storage)**: Query Parquet files in S3, verify schema auto-detection works
4. **US4 (OLAP)**: Query ClickHouse with aggregation, verify sub-second response
5. **US5 (Domestic DBs)**: Query TiDB with Chinese characters, verify no corruption
6. **US6 (File Systems)**: Query HDFS with Kerberos auth, verify successful authentication

### MVP Scope (Recommended)

**Minimum Viable Product**: Phases 1-3 (Setup + Foundational + User Story 1)
- **Tasks**: 1-43 (43 tasks total)
- **Duration**: ~2-3 weeks (estimated)
- **Deliverable**: Full support for Apache Iceberg, Delta Lake, and Apache Hudi
- **Value**: Enables petabyte-scale data lake analytics immediately
- **Risk**: Lowest complexity (3 drivers, well-documented APIs)

**Incremental Delivery Strategy**:
1. **Sprint 1**: MVP (Phases 1-3) - Data lake support (highest value, lowest risk)
2. **Sprint 2**: Phase 4 - Cloud warehouse support (high demand)
3. **Sprint 3**: Phase 5 - Object storage support (high value for exploratory analytics)
4. **Sprint 4**: Phase 6 - OLAP support (specialized use case)
5. **Sprint 5**: Phases 7-8 - Domestic databases + file systems (market-specific)
6. **Sprint 6**: Phase 9 - Polish, optimization, hardening

### Implementation Strategy

**MVP-First Approach**:
- Focus on User Story 1 (Data Lakes) for initial release
- Deliver working software to users early
- Gather feedback before implementing remaining stories
- Each story independently valuable and testable

**Risk Mitigation**:
- Start with best-documented drivers (Iceberg, Snowflake, ClickHouse)
- Develope domestic database drivers last (may require vendor-specific support)
- Use testcontainers for isolated integration testing
- Implement comprehensive contract tests to prevent API breakage

**Quality Gates**:
- All contract tests must pass before merging
- Integration tests must pass for at least one driver per story
- P95 latency targets must be met (data lakes <30s, warehouses <60s, OLAP <5s)
- Security audit: Read-only enforcement verified across all drivers
- Chinese character encoding verified for domestic databases

---

**Ready for Implementation!** 🚀

All tasks follow the checklist format with exact file paths. Tests are included per constitution requirement (TDD mandatory). Tasks are organized by user story for independent implementation and testing.
