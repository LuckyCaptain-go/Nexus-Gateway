# ADR-001: Federation engine adapter contract

- Status: Accepted
- Date: 2026-10-16
- Related: #159, #160

## Context

Data Fabric is written in Go and already routes data-source work through its `drivers.Driver` abstraction and `database/sql`. Java JDBC JARs cannot be loaded directly as Go `database/sql` drivers. Federated planning and cross-catalog execution belong in an external query engine; Data Fabric should remain the gateway and connection adapter.

## Decision

Use the Go `github.com/prestodb/presto-go-client/v2` client through `database/sql` for the shared statement HTTP protocol. Do not load or require a Java JDBC JAR.

The query-engine connection adapter implements the existing `internal/database/drivers.Driver` contract:

- `ValidateDSN` validates endpoint and required connection properties before opening a connection.
- `BuildDSN` maps the gateway configuration to a protocol connection URL.
- `Open` returns a `*sql.DB`; callers execute statements using the standard `database/sql` query APIs and request context.
- `TestConnection` performs a bounded `SELECT 1` health check.
- `GetCapabilities` reports the operations exposed by the adapter.
- Query columns and values are exposed by `database/sql.Rows`, keeping result normalization in the existing query/streaming layers.

One shared protocol implementation supports the configured engine type and selects the appropriate HTTP header family: Trino uses `X-Trino-*`; Presto and OpenLooKeng use `X-Presto-*`. This shared transport is an implementation choice, not a promise that every engine version or connector has identical behavior. The current tests use mock HTTP coordinators; real coordinator compatibility remains an integration-test concern.

Use the existing `DataSourceConfig` fields for endpoint configuration:

| Field | Meaning for a federation engine |
| --- | --- |
| `host` | Coordinator host |
| `port` | Coordinator port; defaults to 8080 |
| `database` | Default catalog |
| `schema` | Optional default schema |
| `username` | Protocol user |
| `password` | Optional password; password authentication requires TLS |
| `ssl` | Use HTTPS |
| `additionalProps` | Protocol properties and TLS options such as CA/client certificate paths |

Catalogs are configured in the coordinator. A query can address another catalog using the engine qualified table-name syntax. Data Fabric does not discover or provision catalogs as part of this adapter contract.

SQL dialect translation remains a separate layer. The adapter sends the SQL provided by the query layer and does not claim universal dialect equivalence.

## Consequences

- The gateway adds no embedded planner or coordinator and can use connectors configured in the engine for cross-catalog execution.
- Standard `database/sql` behavior keeps query execution and result handling consistent with the rest of the gateway.
- Engine-specific functions, type mappings, authentication, TLS behavior, and SQL compatibility still need targeted verification.
- Endpoint registration/health-check API wiring, query routing, cancellation, and real cross-catalog integration are handled by follow-up issues #161-#164; dialect regression coverage is handled by #165.
