"""
Raw to Bronze ingestion pipeline.

This script ingests raw transactional data from a Databricks-managed
catalog table and persists it as a Bronze Delta table following the
medallion architecture pattern.

Environment:
- Databricks Community Edition
- PySpark
- Delta Lake
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp


def main():
    spark = SparkSession.builder.getOrCreate()

    # Source table created via Databricks catalog
    source_table = "online_retail_dataset"

    # Target Bronze table
    bronze_table = "bronze_online_retail"

    # Read raw data from catalog
    raw_df = spark.table(source_table)

    # Add ingestion metadata
    bronze_df = raw_df.withColumn(
        "ingestion_timestamp",
        current_timestamp()
    )

    # Write to Bronze Delta table
    (
        bronze_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(bronze_table)
    )


if __name__ == "__main__":
    main()
