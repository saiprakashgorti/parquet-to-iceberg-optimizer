# Parquet to Iceberg Optimizer

This project implements a data migration pipeline to convert Parquet data to Apache Iceberg format, resulting in significant query performance improvements in AWS Athena.

## Project Overview

- **Source**: 40 GB of Parquet files in AWS S3
- **Target**: Partitioned Apache Iceberg tables in AWS S3
- **Performance Improvement**: 4x reduction in query latency (900ms → 220ms)
- **Technologies**: AWS Glue, PySpark, Apache Iceberg, AWS Athena

## Project Structure

```
.
├── README.md
├── src/
│   ├── glue_jobs/
│   │   └── parquet_to_iceberg_migration.py
│   └── athena/
│       └── table_definitions.sql
├── config/
│   └── spark_config.py
└── requirements.txt
```

## Key Components

1. **AWS Glue ETL Job**
   - PySpark script for data migration
   - Handles Parquet to Iceberg conversion
   - Implements partitioning strategy
   - Manages Glue Data Catalog integration

2. **Spark/Iceberg Configuration**
   - Essential Spark session settings
   - Iceberg-specific configurations
   - Catalog and warehouse settings

3. **Athena Table Definitions**
   - DDL for source Parquet tables
   - DDL for target Iceberg tables
   - Partition management

## Setup and Usage

1. Configure AWS credentials and permissions
2. Set up required AWS services (Glue, S3, Athena)
3. Deploy the Glue job
4. Execute the migration
5. Verify table creation and query performance

## Performance Optimization

The migration achieves query acceleration through:
- Efficient partitioning strategy
- Iceberg's metadata optimization
- Improved file organization
- Better query planning in Athena

## Requirements

- Python 3.8+
- Apache Spark 3.3+
- AWS Glue 3.0+
- Apache Iceberg 0.14+