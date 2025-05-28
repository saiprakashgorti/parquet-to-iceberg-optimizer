"""
Spark configuration settings for Iceberg integration in AWS Glue
"""

# Essential Spark configurations for Iceberg
SPARK_CONFIGS = {
    # Iceberg Spark Session Extensions
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    # Catalog Configuration
    "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
    "spark.sql.catalog.glue_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    # Performance Optimizations
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.sql.adaptive.localShuffleReader.enabled": "true",
    # Iceberg-specific optimizations
    "spark.sql.iceberg.handle-timestamp-without-timezone": "true",
    "spark.sql.iceberg.vectorization.enabled": "true",
    # Memory and execution settings
    "spark.memory.offHeap.enabled": "true",
    "spark.memory.offHeap.size": "10g",
    "spark.sql.shuffle.partitions": "200",
    "spark.default.parallelism": "200",
}

# Table properties for Iceberg tables
ICEBERG_TABLE_PROPERTIES = {
    "write.format.default": "parquet",
    "write.parquet.compression-codec": "zstd",
    "write.parquet.compression-level": "3",
    "write.parquet.row-group-size-bytes": "134217728",
    "write.parquet.page-size-bytes": "1048576",
    "write.target-file-size-bytes": "536870912",
    "write.parquet.dict-size-bytes": "2097152",
    "write.parquet.bloom-filter-enabled.column.id": "true",
    "write.parquet.bloom-filter-max-bytes": "1048576",
}


def get_spark_configs():
    """Return the complete Spark configuration dictionary"""
    return SPARK_CONFIGS


def get_iceberg_table_properties():
    """Return the Iceberg table properties dictionary"""
    return ICEBERG_TABLE_PROPERTIES
