-- Query to compare performance between Parquet and Iceberg tables
WITH parquet_metrics AS (
    SELECT
        query_id,
        query_execution_time_ms,
        data_scanned_bytes,
        'parquet' as table_format
    FROM
        system.query_history
    WHERE
        database_name = 'source_database'
        AND table_name = 'source_parquet_table'
        AND query_execution_time_ms > 0
        AND start_time >= date_add('day', -7, current_date)
),
iceberg_metrics AS (
    SELECT
        query_id,
        query_execution_time_ms,
        data_scanned_bytes,
        'iceberg' as table_format
    FROM
        system.query_history
    WHERE
        database_name = 'target_database'
        AND table_name = 'target_iceberg_table'
        AND query_execution_time_ms > 0
        AND start_time >= date_add('day', -7, current_date)
)
SELECT
    table_format,
    COUNT(*) as query_count,
    AVG(query_execution_time_ms) as avg_execution_time_ms,
    MIN(query_execution_time_ms) as min_execution_time_ms,
    MAX(query_execution_time_ms) as max_execution_time_ms,
    AVG(data_scanned_bytes) as avg_data_scanned_bytes
FROM
    (
        SELECT
            *
        FROM
            parquet_metrics
        UNION
        ALL
        SELECT
            *
        FROM
            iceberg_metrics
    )
GROUP BY
    table_format
ORDER BY
    table_format;

-- Query to analyze partition effectiveness
SELECT
    year,
    month,
    day,
    COUNT(*) as record_count,
    COUNT(DISTINCT id) as unique_ids,
    MIN(creation_date) as earliest_date,
    MAX(creation_date) as latest_date
FROM
    target_database.target_iceberg_table
GROUP BY
    year,
    month,
    day
ORDER BY
    year,
    month,
    day;

-- Query to identify potential optimization opportunities
SELECT
    query_id,
    query_execution_time_ms,
    data_scanned_bytes,
    query_text,
    start_time
FROM
    system.query_history
WHERE
    database_name = 'target_database'
    AND table_name = 'target_iceberg_table'
    AND query_execution_time_ms > 1000 -- Queries taking more than 1 second
    AND start_time >= date_add('day', -7, current_date)
ORDER BY
    query_execution_time_ms DESC
LIMIT
    10;