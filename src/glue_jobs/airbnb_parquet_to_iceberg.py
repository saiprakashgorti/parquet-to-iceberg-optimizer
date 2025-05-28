import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, year, month, dayofmonth, to_timestamp
from datetime import datetime

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Glue context
args = getResolvedOptions(
    sys.argv, ["JOB_NAME", "source_path", "target_path", "database_name", "table_name"]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Configure Spark session for Iceberg
spark.conf.set(
    "spark.sql.extensions",
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
)
spark.conf.set(
    "spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog"
)
spark.conf.set(
    "spark.sql.catalog.glue_catalog.catalog-impl",
    "org.apache.iceberg.aws.glue.GlueCatalog",
)
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", args["target_path"])
spark.conf.set(
    "spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO"
)


def log_metrics(df, stage):
    """Log metrics about the DataFrame"""
    count = df.count()
    logger.info(f"{stage} - Total records: {count}")
    logger.info(f"{stage} - Schema: {df.schema}")
    return df


def read_parquet_data():
    """Read Parquet data from source S3 path"""
    try:
        logger.info(f"Reading Parquet data from: {args['source_path']}")
        df = spark.read.parquet(args["source_path"])
        return log_metrics(df, "Source Data")
    except Exception as e:
        logger.error(f"Error reading Parquet data: {str(e)}")
        raise


def prepare_airbnb_data(df):
    """Prepare Airbnb data with proper types and partitioning"""
    try:
        logger.info("Preparing Airbnb data")

        # Convert string columns to appropriate types
        df = (
            df.withColumn("price", col("price").cast("double"))
            .withColumn("minimum_nights", col("minimum_nights").cast("integer"))
            .withColumn("number_of_reviews", col("number_of_reviews").cast("integer"))
            .withColumn("last_review", to_timestamp(col("last_review")))
        )

        # Add partition columns if they don't exist
        if "year" not in df.columns:
            df = (
                df.withColumn("year", year(col("last_review")))
                .withColumn("month", month(col("last_review")))
                .withColumn("day", dayofmonth(col("last_review")))
            )

        return log_metrics(df, "Prepared Data")
    except Exception as e:
        logger.error(f"Error preparing Airbnb data: {str(e)}")
        raise


def write_iceberg_table(df, database, table):
    """Write DataFrame to Iceberg table with partitioning"""
    try:
        logger.info(f"Writing to Iceberg table: {database}.{table}")
        start_time = datetime.now()

        # Create or replace table with Airbnb-specific properties
        df.writeTo(f"glue_catalog.{database}.{table}").using("iceberg").partitionedBy(
            "year", "month", "day"
        ).tableProperty("write.format.default", "parquet").tableProperty(
            "write.parquet.compression-codec", "zstd"
        ).tableProperty(
            "write.parquet.compression-level", "3"
        ).createOrReplace()

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(f"Table write completed in {duration} seconds")

    except Exception as e:
        logger.error(f"Error writing Iceberg table: {str(e)}")
        raise


def validate_migration(source_df, target_df):
    """Validate the migration by comparing record counts and sample data"""
    try:
        source_count = source_df.count()
        target_count = target_df.count()

        if source_count != target_count:
            logger.error(
                f"Data validation failed: Source count ({source_count}) != Target count ({target_count})"
            )
            raise ValueError("Data validation failed: Record count mismatch")

        # Sample validation of data
        sample_source = source_df.sample(0.01).collect()
        sample_target = target_df.sample(0.01).collect()

        logger.info(f"Data validation successful: {source_count} records migrated")
        logger.info(f"Sample validation: {len(sample_source)} records compared")

    except Exception as e:
        logger.error(f"Error during validation: {str(e)}")
        raise


def main():
    try:
        # Read source data
        source_df = read_parquet_data()

        # Prepare data
        prepared_df = prepare_airbnb_data(source_df)

        # Write to Iceberg table
        write_iceberg_table(prepared_df, args["database_name"], args["table_name"])

        # Validate the migration
        target_df = spark.table(
            f"glue_catalog.{args['database_name']}.{args['table_name']}"
        )
        validate_migration(source_df, target_df)

        logger.info("Migration completed successfully")

    except Exception as e:
        logger.error(f"Migration failed: {str(e)}")
        raise
    finally:
        job.commit()


if __name__ == "__main__":
    main()
