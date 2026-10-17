# Configuration Guide

## Overview

Data Fabric is configured primarily through a YAML configuration file located at `configs/config.yaml`. This document explains all available configuration options and their purposes.

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
  username: "data_fabric"
  password: "your_password"
  database: "data_fabric"

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
| `database.username` | String | Username for internal database | data_fabric |
| `database.password` | String | Password for internal database | your_password |
| `database.database` | String | Database name for internal database | data_fabric |

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

## Data-source credential encryption

The HTTP and MCP servers require an encryption keyring at startup. Keys are injected by the runtime or a secrets manager; they are not stored in the repository or the YAML config. If either variable is missing, malformed, or does not include the active key, startup fails.

Generate a 32-byte key with openssl rand -base64 32 and store it in your secret manager. Inject the active key ID and a JSON map of key IDs to base64 keys:

    export DATA_FABRIC_CREDENTIAL_ACTIVE_KEY_ID=key-v1
    export DATA_FABRIC_CREDENTIAL_KEYS='{"key-v1":"<base64-encoded-32-byte-key>"}'

Use the same key across restarts. Generating a new key without retaining the old key makes previously encrypted data-source credentials unreadable.

At startup, Data Fabric encrypts legacy plaintext credentials and validates existing ciphertext in a database transaction before accepting requests. If migration fails, the transaction is rolled back and the server does not start. Data-source passwords and recognized secret values in additionalProps are encrypted before persistence and decrypted only when loaded for internal connection use.

### Key rotation

1. Keep the current key in the keyring and add a newly generated key under a new ID.
2. Set DATA_FABRIC_CREDENTIAL_ACTIVE_KEY_ID to the new ID while retaining both keys in DATA_FABRIC_CREDENTIAL_KEYS.
3. Restart the service. Startup migrates all data-source credentials to the new key; any missing or invalid old key stops startup.
4. After the migration has completed successfully on every deployment, remove the old key from the injected keyring.

Back up the database and key material securely before rotation. Do not put actual key values in config files, shell scripts, or source control.

## Secret management

`configs/config.yaml` is a local, untracked file. Start from `configs/config.yaml.example` (placeholder values only) and keep real secrets out of it:

1. Environment variables win over the config file for the secret fields (`DATABASE_USERNAME`, `DATABASE_PASSWORD`, `JWT_SECRET`). Prefer them in every deployment.
2. The `.env` file is gitignored but **not loaded automatically**. To use one locally: `set -a; source .env; set +a`, or pass `--env-file` to `docker run` / `env_file` in compose.
3. In container deployments, inject secrets through the platform's secret mechanism (Kubernetes Secret, Docker secrets) instead of baking config files into images — the published image ships `config.yaml.example` only.
4. Calling `/api/v1/fetch` always needs a Bearer token, regardless of `security.enable_auth`. There is no token-issuing endpoint yet, so mint one yourself with the same `JWT_SECRET` (requires `pyjwt`):

   ```bash
   python3 -c "import jwt,time; print(jwt.encode({'userId':'dev','username':'dev','roles':['admin'],'iat':int(time.time()),'exp':int(time.time())+86400}, 'YOUR_JWT_SECRET', algorithm='HS256'))"
   ```

   If `JWT_SECRET` is unset the server falls back to a source-code default that anyone can read — never rely on that in shared environments (#8 removes the fallback).

## Local development vs production

| Setting | Local development (`debug`/`test`) | Production (`release`) |
|---|---|---|
| `security.enable_auth` | May be `false` to leave the data-source management API unauthenticated; `/api/v1/fetch` still requires a Bearer token either way (#6) | Must be `true`; the server refuses to start otherwise |
| `security.jwt_secret` | Required whenever the HTTP server runs: set, not a known placeholder, at least 32 bytes | Same |
| `server.mode` | `debug` / `test` | `release` |

Startup fails closed on insecure authentication settings before the metadata database is connected. `test_fetch_api.sh` reads the Bearer token from `FETCH_TOKEN` — mint one as shown under Secret management and export it before running the script.

## Environment Variables

Secret fields are injected through environment variables and take precedence over any value in the config file. These variables are honored today:

| Configuration Path | Environment Variable | Status |
|-------------------|----------------------|--------|
| `database.username` | `DATABASE_USERNAME` | Supported |
| `database.password` | `DATABASE_PASSWORD` | Supported |
| `security.jwt_secret` | `JWT_SECRET` | Supported |

Other overrides previously listed here (`SERVER_PORT`, `SERVER_MODE`, `DATABASE_HOST`, `DATABASE_PORT`, `DATABASE_NAME`, rate-limit and query toggles) are not wired up yet; the full mapping is tracked in #126.

Example:
```bash
export DATABASE_USERNAME=data_fabric
export DATABASE_PASSWORD=secure-password
export JWT_SECRET=$(openssl rand -base64 32)
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