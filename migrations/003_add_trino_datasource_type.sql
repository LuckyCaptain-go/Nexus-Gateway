-- Add Trino to existing data source schemas.
ALTER TABLE data_sources MODIFY COLUMN type ENUM(
    'mysql', 'mariadb', 'postgresql', 'oracle', 'trino',
    'iceberg', 'delta_lake', 'hudi', 'snowflake', 'databricks', 'redshift', 'bigquery',
    's3_parquet', 's3_orc', 's3_avro', 's3_csv', 's3_json', 'minio_parquet', 'minio_csv',
    'oss_parquet', 'cos_parquet', 'azure_blob_parquet', 'hdfs_avro', 'hdfs_parquet',
    'hdfs_csv', 'ozone_parquet', 'clickhouse', 'doris', 'starrocks', 'druid',
    'oceanbase_mysql', 'oceanbase_oracle', 'tidb', 'tdsql', 'gaussdb_mysql',
    'gaussdb_postgres', 'dameng', 'kingbasees', 'gbase_8s', 'gbase_8t', 'oscar',
    'opengauss', 'oss_delta', 'minio_iceberg', 'azure_delta', 'ozone_text', 'ozone_avro',
    'azure_parquet', 'hdfs_text', 'hdfs_parquet_compressed', 'minio_delta', 'hive',
    'sqlserver', 'hologres', 'mongodb', 'maxcompute'
) NOT NULL;
