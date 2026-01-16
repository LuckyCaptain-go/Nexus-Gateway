# Configuration Guide

## Overview

Nexus-Gateway is configured primarily through a YAML configuration file located at `configs/config.yaml`. This document explains all available configuration options and their purposes.

## Main Configuration File

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

## Configuration Sections

### Server Configuration

| Field | Type | Description | Default |
|-------|------|-------------|---------|
| `server.port` | Integer | Port on which the gateway listens | 8099 |
| `server.mode` | String | Gin framework mode (debug/release/production) | release |
| `server.host` | String | Host IP address to bind to | "0.0.0.0" |

### Database Configuration

| Field | Type | Description | Default |
|-------|------|-------------|---------|
| `database.host` | String | Hostname of the internal database | localhost |
| `database.port` | Integer | Port of the internal database | 3306 |
| `database.username` | String | Username for internal database | nexus |
| `database.password` | String | Password for internal database | your_password |
| `database.database` | String | Database name for internal database | nexus_gateway |

### Security Configuration

#### JWT Settings
| Field | Type | Description | Default |
|-------|------|-------------|---------|
| `security.jwt.secret` | String | Secret key for JWT signing | your-jwt-secret |
| `security.jwt.expiration` | Duration | Token expiration time | 24h |

#### Rate Limiting Settings
| Field | Type | Description | Default |
|-------|------|-------------|---------|
| `security.rateLimit.enabled` | Boolean | Enable rate limiting | true |
| `security.rateLimit.requests` | Integer | Max requests per window | 100 |
| `security.rateLimit.window` | Duration | Time window for rate limiting | 1m |

### Logging Configuration

| Field | Type | Description | Default |
|-------|------|-------------|---------|
| `logging.level` | String | Log level (debug/info/warn/error) | info |
| `logging.format` | String | Log format (json/text) | json |

### Query Configuration

| Field | Type | Description | Default |
|-------|------|-------------|---------|
| `query.prefer_streaming` | Boolean | Prefer streaming cursors for large result sets | true |

## Driver-Specific Configuration

### Cloud Data Warehouses

#### Snowflake
| Field | Type | Description |
|-------|------|-------------|
| `drivers.snowflake.account` | String | Snowflake account identifier |
| `drivers.snowflake.warehouse` | String | Snowflake warehouse name |

#### Databricks
| Field | Type | Description |
|-------|------|-------------|
| `drivers.databricks.workspace_url` | String | Workspace URL |
| `drivers.databricks.http_path` | String | HTTP path for SQL endpoint |

### Object Storage

#### AWS Credentials
| Field | Type | Description |
|-------|------|-------------|
| `drivers.aws.region` | String | AWS region |
| `drivers.aws.access_key_id` | String | AWS access key ID |
| `drivers.aws.secret_access_key` | String | AWS secret access key |

#### Azure Credentials
| Field | Type | Description |
|-------|------|-------------|
| `drivers.azure.account_name` | String | Azure storage account name |
| `drivers.azure.account_key` | String | Azure storage account key |

### Domestic Databases

#### OceanBase
| Field | Type | Description |
|-------|------|-------------|
| `drivers.oceanbase.mode` | String | Compatibility mode (mysql/oracle) |

#### TiDB
| Field | Type | Description |
|-------|------|-------------|
| `drivers.tidb.pd_addresses` | Array | List of PD addresses |

## Environment Variables

Configuration can also be overridden using environment variables:

| Configuration Path | Environment Variable |
|-------------------|---------------------|
| `server.port` | `SERVER_PORT` |
| `server.mode` | `SERVER_MODE` |
| `database.host` | `DATABASE_HOST` |
| `database.port` | `DATABASE_PORT` |
| `database.username` | `DATABASE_USERNAME` |
| `database.password` | `DATABASE_PASSWORD` |
| `database.database` | `DATABASE_NAME` |
| `security.jwt.secret` | `JWT_SECRET` |
| `security.rateLimit.enabled` | `RATELIMIT_ENABLED` |
| `query.prefer_streaming` | `QUERY_PREFER_STREAMING` |

Example:
```bash
export SERVER_MODE=production
export DATABASE_HOST=your-db-host
export DATABASE_PASSWORD=secure-password
export JWT_SECRET=your-jwt-secret
```

## Configuration Validation

The system performs validation on startup:
- Required fields are checked
- Connection parameters are validated
- Security settings are verified
- Driver configurations are tested

Any configuration errors will cause the application to exit with an informative error message.

## Security Considerations

### Credential Management
- Store sensitive credentials in environment variables rather than config files
- Use secrets management systems in production
- Regularly rotate credentials
- Apply principle of least privilege

### Production Settings
- Set server mode to `release` in production
- Use strong JWT secrets
- Configure appropriate rate limiting
- Enable secure logging practices
- Use TLS for all communications