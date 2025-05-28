-- Source Parquet Table Definition
CREATE EXTERNAL TABLE IF NOT EXISTS source_database.source_parquet_table (
    id STRING,
    name STRING,
    creation_date TIMESTAMP,
    value DOUBLE,
    category STRING
) PARTITIONED BY (year INT, month INT, day INT) STORED AS PARQUET LOCATION 's3://your-source-bucket/path/to/parquet/' TBLPROPERTIES ('has_encrypted_data' = 'false');

-- Target Iceberg Table Definition
CREATE EXTERNAL TABLE IF NOT EXISTS target_database.target_iceberg_table (
    id STRING,
    name STRING,
    creation_date TIMESTAMP,
    value DOUBLE,
    category STRING
) PARTITIONED BY (year INT, month INT, day INT) LOCATION 's3://your-target-bucket/path/to/iceberg/' TBLPROPERTIES (
    'table_type' = 'ICEBERG',
    'format' = 'iceberg',
    'write_compression' = 'zstd',
    'write_target_file_size_bytes' = '536870912'
);

-- Example query to verify data migration
SELECT
    COUNT(*) as total_records,
    MIN(creation_date) as earliest_date,
    MAX(creation_date) as latest_date
FROM
    target_database.target_iceberg_table
WHERE
    year = 2024
GROUP BY
    year,
    month,
    day
ORDER BY
    year,
    month,
    day;