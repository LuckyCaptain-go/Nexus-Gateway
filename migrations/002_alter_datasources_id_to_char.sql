-- Alter data_sources table to change id from BINARY(16) to CHAR(36)
-- This migration changes the UUID storage from binary to string format

-- First, we need to drop the primary key constraint
ALTER TABLE data_sources DROP PRIMARY KEY;

-- Change the column type from BINARY(16) to CHAR(36)
ALTER TABLE data_sources MODIFY id CHAR(36) NOT NULL COMMENT 'UUID stored as string';

-- Re-add the primary key constraint
ALTER TABLE data_sources ADD PRIMARY KEY (id);

