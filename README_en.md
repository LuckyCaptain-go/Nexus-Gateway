# Nexus-Gateway: A Lightweight Heterogeneous Data Access Gateway

<div align="center">
  <img src="assets/nexus-gateway-icon.png" alt="Nexus-Gateway icon" width="176">
  <p><strong>Lightweight access to heterogeneous data through one application-facing API</strong></p>
  <p><a href="README.md">Project home</a> · <a href="README_zh.md">中文介绍</a></p>
  <p>
    <img src="https://img.shields.io/badge/Go-1.25.1+-00ADD8?logo=go" alt="Go 1.25.1+">
    <img src="https://img.shields.io/badge/License-Apache--2.0-blue" alt="Apache 2.0">
  </p>
</div>

Nexus-Gateway is for AI agents, RAG systems, BI tools, and business applications that need heterogeneous data access. It addresses repeated integration work: each application should not have to maintain separate drivers, connection settings, query calls, and result pagination for every database, warehouse, data lake, object store, or file system.

The gateway brings those application-facing tasks behind one data access API. Applications manage connections, inspect sources, and retrieve query results through Nexus. When they need SQL across multiple systems, a query-engine adapter can extend the gateway with federated query execution.

## Product Niche

**Nexus-Gateway is a lightweight data access entry point between applications and data systems.** It handles application-facing APIs, source connection lifecycle, and gateway controls. Cross-source query engines can be connected through adapters when needed for planning and executing federated SQL. The original data systems continue to own and store their data.

| Component | Responsibility |
| --- | --- |
| Nexus-Gateway | Application-facing API, source connection management, diagnostics, single-source result retrieval, and gateway controls |
| Federation engine | Access multiple systems through catalogs and connectors, then plan and execute cross-source SQL |
| Source systems | Store and manage the actual data |

### Why Lightweight

- **No extra engine for single-source access:** the current gateway can use integrated drivers to access individual sources without deploying a federation engine for a simple connectivity need.
- **Add federation when needed:** cross-source joins can use a federation engine and its connectors when the application needs them.
- **Reduce repeated integration work:** applications use a consistent API instead of implementing connection and pagination logic for every source.
- **Avoid copying all data:** federated queries can leave data in their source systems, though query execution still involves network traffic and intermediate result exchange.
- **Lightweight deployment is a product goal:** the project targets a small gateway service, low idle resource use, and a short deployment path. The repository does not publish resource or performance benchmarks, so it makes no numeric guarantees for memory, CPU, or startup time.

### Nexus-Gateway Responsibilities

Nexus-Gateway focuses on application-facing connectivity, a consistent access API, source lifecycle, and the request entry point. It is not another distributed SQL engine, database, warehouse, ETL, or data-copy platform.

A deployment can start in lightweight gateway mode for individual sources and add federated execution when cross-source SQL becomes necessary. Whether a query engine is deployed depends on the application's federation needs.

## Current Implementation Status

The repository implements gateway APIs, data-source management, driver registration, connection diagnostics, configuration validation, JWT and rate-limiting components, and batched query result retrieval. Drivers with streaming support can read results in batches instead of loading the entire result set into memory at once.

> **Federation engine adapters are not implemented yet.** The server request path does not include a federated query execution adapter, and it does not automatically manage external engine catalogs. The current version can access individual integrated sources through the gateway, but cannot yet run cross-source federated SQL through Nexus-Gateway. The dotted line in the diagram marks a target integration boundary, not a shipped capability.

~~~mermaid
flowchart LR
    App[AI Agent / RAG / BI / Application] --> Gateway[Nexus-Gateway<br/>Application API · Source Management]
    Gateway --> Drivers[Current: Drivers and Single-Source Access]
    Drivers --> Sources[Databases / Warehouses / Lakehouses / Object Storage / Files]
    Gateway -. Target: Federation Adapter .-> Engine[Cross-Source Query Engine<br/>SQL Planning and Distributed Execution]
    Engine -.-> Catalogs[Catalogs and Connectors]
    Catalogs -.-> Sources
~~~

Solid arrows represent gateway and single-source paths in the current repository. The dotted arrow is the target federation-engine integration boundary. An adapter will need to define the application query protocol, catalog lifecycle, identity propagation, query cancellation, timeouts, and result flow.

## Gateway Capabilities

- **Data-source management:** Create, inspect, update, activate, deactivate, and delete data sources.
- **Driver extension:** Add source types through the common driver interface and registry.
- **Connection diagnostics:** Test connections, validate configuration, and inspect connection statistics and health.
- **Query results:** Retrieve data through the gateway query API and fetch subsequent result batches; streaming-capable drivers can reduce memory use for large results.
- **Source-type discovery:** Inspect the source types recognized by the running service.
- **Gateway controls:** The codebase includes JWT, request rate limiting, configuration validation, and health-check components. Actual authentication coverage depends on deployment configuration and route groups.

## Data Source Categories

The following are representative source types defined in the repository. Driver completeness, authentication methods, available features, and whether a driver is enabled in a given build may vary. Check the running service's type endpoint and the driver implementation.

| Category | Representative types |
| --- | --- |
| Relational and compatible databases | MySQL, MariaDB, PostgreSQL, Oracle, SQL Server |
| Domestic databases | OceanBase, TiDB, TDSQL, GaussDB, DaMeng, KingbaseES, GBase, Oscar, OpenGauss |
| Analytical databases | ClickHouse, Doris, StarRocks, Druid, Hologres |
| Cloud warehouses and big-data platforms | Snowflake, Databricks, Redshift, BigQuery, Hive, MaxCompute |
| Lakehouse table formats | Apache Iceberg, Delta Lake, Apache Hudi |
| Object storage and file systems | S3, MinIO, Alibaba OSS, Tencent COS, Azure Blob, HDFS, Apache Ozone |
| File formats | Parquet, ORC, Avro, CSV, JSON, XML, text |

## Quick Start

### Requirements

- Go 1.25.1 or newer.
- A metadata database configured for the service and reachable data sources.
- Single-source gateway mode does not require a federation engine. Federated queries require an implemented engine adapter and a deployed engine.

### Get the Code and Start

~~~bash
git clone https://github.com/LuckyCaptain-go/Nexus-Gateway.git
cd Nexus-Gateway
go mod download
~~~

Review the service port, metadata database, and security settings in configs/config.yaml, then start the server:

~~~bash
go run ./cmd/server
~~~

The health-check path is /health. See the [configuration guide](docs/configuration.md) for defaults and deployment options.

> The checked-in credentials are examples. The server bootstrap currently uses a fixed credential-encryption master key; replace it with secure external secret management before production, and verify that authentication covers the routes that need protection.

## API Overview

| Method | Path | Purpose |
| --- | --- | --- |
| GET | /health | Service health check |
| GET | /api/v1/database/types | List source types recognized by the service |
| GET | /api/v1/database/compatibility | Inspect database compatibility information |
| GET / POST | /api/v1/datasources | List or create data sources |
| GET / PUT / DELETE | /api/v1/datasources/:id | Inspect, update, or delete a data source |
| POST | /api/v1/datasources/:id/activate | Activate a data source |
| POST | /api/v1/datasources/:id/deactivate | Deactivate a data source |
| POST | /api/v1/database/test-connection | Test a data-source connection |
| POST | /api/v1/database/validate-config | Validate data-source configuration |
| POST | /api/v1/fetch | Execute a batch query and fetch its first result batch |
| GET | /api/v1/fetch/:query_id/:slug/:token | Fetch a subsequent result batch |

See the [API usage guide](docs/api_usage.md) for request fields, response structures, and authentication behavior.

## Documentation

- [API Usage Guide](docs/api_usage.md)
- [Architecture Guide](docs/architecture.md)
- [Configuration Guide](docs/configuration.md)
- [Development Guide](docs/development.md)
- [Contributing Guide](docs/contributing.md)
- [中文 README](README_zh.md)

## Contributing

If Nexus-Gateway is useful to you, please star the repository. You are also welcome to contribute through issues, pull requests, and documentation improvements.

## Federation Engine Integrations

The project aims to provide a common, seamless integration path for multiple cross-source query engines, including Trino, Presto, and OpenLooKeng. These are future adapter targets; the current repository does not yet ship a federated query adapter.

## License

This project is licensed under Apache License 2.0. See [LICENSE](LICENSE).