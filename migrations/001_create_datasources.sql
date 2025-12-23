-- Create data_sources table for storing database connection configurations
CREATE TABLE data_sources (
    id CHAR(36) PRIMARY KEY COMMENT 'UUID stored as string',
    name VARCHAR(255) NOT NULL COMMENT 'Human readable name for the data source',
    type ENUM('mysql', 'mariadb', 'postgresql', 'oracle') NOT NULL COMMENT 'Database type',
    config JSON NOT NULL COMMENT 'Connection configuration in JSON format',
    status ENUM('active', 'inactive', 'error') DEFAULT 'active' COMMENT 'Current status of the data source',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT 'Creation timestamp',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Last update timestamp',

    -- Indexes for better query performance
    INDEX idx_status (status),
    INDEX idx_type (type),
    INDEX idx_created_at (created_at),

    -- Unique constraint on name
    UNIQUE KEY uk_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='Data source configurations for the gateway';

-- Add comment for the table
ALTER TABLE data_sources COMMENT = 'Stores configuration for various database connections that can be queried through the gateway';