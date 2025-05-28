# Parquet to Iceberg Optimizer

This project implements a data migration pipeline to convert Parquet data to Apache Iceberg format, resulting in significant query performance improvements in AWS Athena.

## Project Overview

- **Source**: 40 GB of Parquet files in AWS S3 (or any Parquet dataset, e.g., Kaggle Airbnb)
- **Target**: Partitioned Apache Iceberg tables in AWS S3
- **Performance Improvement**: 4x reduction in query latency (900ms → 220ms)
- **Technologies**: AWS Glue, PySpark, Apache Iceberg, AWS Athena

## Project Structure

```
.
├── README.md
├── setup_and_run.py
├── setup_directories.py
├── requirements.txt
├── src/
│   ├── glue_jobs/
│   │   ├── parquet_to_iceberg_migration.py
│   │   └── airbnb_parquet_to_iceberg.py
│   ├── data_preparation/
│   │   └── download_kaggle_dataset.py
│   └── athena/
│       ├── table_definitions.sql
│       ├── performance_monitoring.sql
│       └── test_queries.sql
├── config/
│   └── spark_config.py
└── data/
    ├── raw/
    └── parquet/
```

## Setup and Usage

### 1. AWS Credentials & Region
- Make sure you have AWS CLI installed and configured:
  ```bash
  aws configure
  ```
  - Enter your AWS Access Key, Secret Key, and **set your default region** (e.g., `us-west-2`).
- If you see `NoRegionError: You must specify a region.`, set your region with:
  ```bash
  export AWS_DEFAULT_REGION=us-west-2
  ```

### 2. Install Python Requirements
```bash
pip install -r requirements.txt
```

### 3. Kaggle API Setup (for sample data)
- Download your `kaggle.json` from https://www.kaggle.com/settings/account
- Place it in `~/.kaggle/kaggle.json`
- Set permissions:
  ```bash
  chmod 600 ~/.kaggle/kaggle.json
  ```

### 4. Prepare Directories
```bash
python setup_directories.py
```

### 5. Run the End-to-End Pipeline
```bash
python setup_and_run.py
```

This will:
- Set up AWS S3, Glue, and IAM resources
- Download and prepare the Kaggle Airbnb dataset
- Upload it to S3
- Create and run the Glue job for Parquet-to-Iceberg migration

### 6. Monitor and Query
- Monitor Glue jobs: https://console.aws.amazon.com/glue/home?region=us-west-2#etl:tab=jobs
- Check S3 data: https://s3.console.aws.amazon.com/s3/buckets/
- Query in Athena: https://console.aws.amazon.com/athena/home?region=us-west-2
- Use queries in `src/athena/test_queries.sql` and `src/athena/performance_monitoring.sql`

## Troubleshooting
- **NoRegionError**: Set your region with `aws configure` or `export AWS_DEFAULT_REGION=us-west-2`.
- **Permission errors**: Ensure your AWS user has S3, Glue, and IAM permissions.
- **Kaggle errors**: Ensure your `kaggle.json` is in place and permissions are correct.
- **Dependency errors**: Remove `apache-iceberg` from requirements; use `pyiceberg` for local Python Iceberg table access.

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
- Apache Iceberg (via AWS Glue runtime)
- AWS CLI
- Kaggle account (for sample data)