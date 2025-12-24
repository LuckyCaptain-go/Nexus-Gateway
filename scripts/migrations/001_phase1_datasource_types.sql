-- Migration 001: Phase 1 Enhanced Single-Source Capabilities
-- Extends data_sources table to support 30+ new data source types
-- Date: 2025-12-24
-- Author: Nexus Gateway Team

-- ============================================================================
-- SECTION 1: EXTEND data_sources.type ENUM
-- ============================================================================

-- For MySQL/MariaDB
ALTER TABLE data_sources
MODIFY COLUMN type ENUM(
    -- Existing types (Phase 0)
    'mysql',
    'mariadb',
    'postgresql',
    'oracle',

    -- Data Lake Table Formats (Phase 1)
    'iceberg',
    'delta_lake',
    'hudi',

    -- Cloud Data Warehouses (Phase 1)
    'snowflake',
    'databricks',
    'redshift',
    'bigquery',

    -- Object Storage (Phase 1)
    's3_parquet',
    's3_orc',
    's3_avro',
    's3_csv',
    's3_json',
    'minio_parquet',
    'minio_csv',
    'oss_parquet',
    'cos_parquet',
    'azure_parquet',

    -- Distributed File Systems (Phase 1)
    'hdfs_avro',
    'hdfs_parquet',
    'hdfs_csv',
    'ozone_parquet',

    -- OLAP Engines (Phase 1)
    'clickhouse',
    'doris',
    'starrocks',
    'druid',

    -- Domestic Chinese Databases (Phase 1)
    'oceanbase_mysql',
    'oceanbase_oracle',
    'tidb',
    'tdsql',
    'gaussdb_mysql',
    'gaussdb_postgres',
    'dameng',
    'kingbasees',
    'gbase_8s',
    'gbase_8t',
    'oscar',
    'opengauss'
) NOT NULL;

-- ============================================================================
-- SECTION 2: CREATE data_source_schema TABLE (optional, for schema caching)
-- ============================================================================

CREATE TABLE IF NOT EXISTS data_source_schema (
    data_source_id CHAR(36) PRIMARY KEY,
    schema_metadata JSON NOT NULL,
    schema_version VARCHAR(50) DEFAULT NULL,
    last_refreshed_at TIMESTAMP NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    FOREIGN KEY (data_source_id)
        REFERENCES data_sources(id)
        ON DELETE CASCADE,

    INDEX idx_schema_version (schema_version),
    INDEX idx_last_refreshed (last_refreshed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================
-- SECTION 3: CREATE credential_vault TABLE (optional, for encrypted credentials)
-- ============================================================================

CREATE TABLE IF NOT EXISTS credential_vault (
    id CHAR(36) PRIMARY KEY,
    data_source_id CHAR(36) UNIQUE NOT NULL,
    encrypted_credentials TEXT NOT NULL,
    encryption_key_id VARCHAR(100) DEFAULT NULL,
    auth_type ENUM('basic', 'oauth2', 'iam', 'kerberos', 'jwt', 'sas', 'pat', 'keypair') NOT NULL,
    token_expires_at TIMESTAMP NULL,
    last_rotated_at TIMESTAMP NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    FOREIGN KEY (data_source_id)
        REFERENCES data_sources(id)
        ON DELETE CASCADE,

    INDEX idx_token_expires (token_expires_at),
    INDEX idx_auth_type (auth_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================
-- SECTION 4: EXTEND query_history TABLE (optional, for enhanced metrics)
-- ============================================================================

-- Check if columns exist before adding them (for re-runnable migrations)
SET @dbname = DATABASE();
SET @tablename = 'query_history';
SET @columnname1 = 'data_processed_bytes';
SET @columnname2 = 'query_hash';
SET @columnname3 = 'snapshot_id';
SET @columnname4 = 'retry_count';

SET @preparedStatement1 = (SELECT IF(
  (
    SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = @dbname
    AND TABLE_NAME = @tablename
    AND COLUMN_NAME = @columnname1
  ) > 0,
  'SELECT 1',
  CONCAT('ALTER TABLE ', @tablename, ' ADD COLUMN ', @columnname1, ' BIGINT DEFAULT NULL COMMENT ''Amount of data scanned in bytes''')
));

SET @preparedStatement2 = (SELECT IF(
  (
    SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = @dbname
    AND TABLE_NAME = @tablename
    AND COLUMN_NAME = @columnname2
  ) > 0,
  'SELECT 1',
  CONCAT('ALTER TABLE ', @tablename, ' ADD COLUMN ', @columnname2, ' CHAR(32) DEFAULT NULL COMMENT ''MD5 hash of normalized SQL''')
));

SET @preparedStatement3 = (SELECT IF(
  (
    SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = @dbname
    AND TABLE_NAME = @tablename
    AND COLUMN_NAME = @columnname3
  ) > 0,
  'SELECT 1',
  CONCAT('ALTER TABLE ', @tablename, ' ADD COLUMN ', @columnname3, ' VARCHAR(100) DEFAULT NULL COMMENT ''Table format snapshot ID''')
));

SET @preparedStatement4 = (SELECT IF(
  (
    SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = @dbname
    AND TABLE_NAME = @tablename
    AND COLUMN_NAME = @columnname4
  ) > 0,
  'SELECT 1',
  CONCAT('ALTER TABLE ', @tablename, ' ADD COLUMN ', @columnname4, ' INT DEFAULT 0 COMMENT ''Number of retries before success/failure''')
));

PREPARE alterIfNotExists1 FROM @preparedStatement1;
EXECUTE alterIfNotExists1;
DEALLOCATE PREPARE alterIfNotExists1;

PREPARE alterIfNotExists2 FROM @preparedStatement2;
EXECUTE alterIfNotExists2;
DEALLOCATE PREPARE alterIfNotExists2;

PREPARE alterIfNotExists3 FROM @preparedStatement3;
EXECUTE alterIfNotExists3;
DEALLOCATE PREPARE alterIfNotExists3;

PREPARE alterIfNotExists4 FROM @preparedStatement4;
EXECUTE alterIfNotExists4;
DEALLOCATE PREPARE alterIfNotExists4;

-- ============================================================================
-- SECTION 5: CREATE INDEXES for performance
-- ============================================================================

-- Data sources indexes
CREATE INDEX IF NOT EXISTS idx_datasources_type ON data_sources(type);
CREATE INDEX IF NOT EXISTS idx_datasources_type_status ON data_sources(type, status);

-- Query history indexes
CREATE INDEX IF NOT EXISTS idx_query_history_user_id ON query_history(user_id);
CREATE INDEX IF NOT EXISTS idx_query_history_status ON query_history(status);
CREATE INDEX IF NOT EXISTS idx_query_history_query_hash ON query_history(query_hash);
CREATE INDEX IF NOT EXISTS idx_query_history_correlation_id ON query_history(correlation_id);

-- ============================================================================
-- ROLLBACK SCRIPT (for rollback purposes)
-- ============================================================================

/*
-- To rollback this migration, use:

-- Drop Phase 1 indexes
DROP INDEX IF EXISTS idx_datasources_type ON data_sources;
DROP INDEX IF EXISTS idx_datasources_type_status ON data_sources;
DROP INDEX IF EXISTS idx_query_history_user_id ON query_history;
DROP INDEX IF EXISTS idx_query_history_status ON query_sources;
DROP INDEX IF EXISTS idx_query_history_query_hash ON query_history;
DROP INDEX IF EXISTS idx_query_history_correlation_id ON query_history;

-- Remove added columns from query_history
ALTER TABLE query_history DROP COLUMN IF EXISTS data_processed_bytes;
ALTER TABLE query_history DROP COLUMN IF EXISTS query_hash;
ALTER TABLE query_history DROP COLUMN IF EXISTS snapshot_id;
ALTER TABLE query_history DROP COLUMN IF EXISTS retry_count;

-- Drop new tables
DROP TABLE IF EXISTS credential_vault;
DROP TABLE IF EXISTS data_source_schema;

-- Revert data_sources.type ENUM (modify to original 4 values)
ALTER TABLE data_sources
MODIFY COLUMN type ENUM('mysql', 'mariadb', 'postgresql', 'oracle') NOT NULL;
*/

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Verify ENUM extension
SELECT
    COLUMN_TYPE as data_source_type_enum,
    COUNT(*) as total_data_sources
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = DATABASE()
AND TABLE_NAME = 'data_sources'
AND COLUMN_NAME = 'type';

-- Verify new tables
SELECT
    TABLE_NAME,
    TABLE_ROWS,
    CREATE_TIME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = DATABASE()
AND TABLE_NAME IN ('data_source_schema', 'credential_vault')
ORDER BY TABLE_NAME;

-- Verify indexes
SHOW INDEX FROM data_sources WHERE Key IN ('idx_datasources_type', 'idx_datasources_type_status');
SHOW INDEX FROM query_history WHERE Key LIKE 'idx_query_history%';
