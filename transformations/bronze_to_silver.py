"""
Bronze to Silver transformation.

Cleans and standardizes raw retail transaction data from the Bronze layer
to produce a reliable Silver Delta table for analytics and downstream use.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main():
    spark = SparkSession.builder.getOrCreate()

    bronze_table = "bronze_online_retail"
    silver_table = "silver_online_retail"

    # Read Bronze data
    bronze_df = spark.table(bronze_table)

    # ---------------------------
    # Cleaning & standardization
    # ---------------------------
    silver_df = (
        bronze_df
        # Remove records without customer information
        .dropna(subset=["CustomerID"])
        # Cast CustomerID to integer type
        .withColumn("CustomerID", col("CustomerID").cast("long"))
        # Remove invalid quantities and prices
        .filter(col("Quantity") > 0)
        .filter(col("UnitPrice") >= 0)
    )

    # ---------------------------
    # Deduplication
    # ---------------------------
    silver_df = silver_df.dropDuplicates(
        ["InvoiceNo", "StockCode"]
    )

    # ---------------------------
    # Write Silver Delta table
    # ---------------------------
    (
        silver_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
    )


if __name__ == "__main__":
    main()
