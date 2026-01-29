"""
Silver to Gold transformations.

Creates analytics-ready Gold tables from clean Silver data,
including customer-level and daily sales aggregates.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum,
    countDistinct,
    avg,
    min,
    max,
    to_date
)


def main():
    spark = SparkSession.builder.getOrCreate()

    silver_table = "silver_online_retail"

    customer_gold_table = "gold_customer_metrics"
    daily_gold_table = "gold_daily_sales"

    # Read Silver data
    silver_df = spark.table(silver_table)

    # ---------------------------
    # Customer-level metrics
    # ---------------------------
    customer_gold_df = (
        silver_df
        .withColumn("order_value", col("Quantity") * col("UnitPrice"))
        .groupBy("CustomerID")
        .agg(
            countDistinct("InvoiceNo").alias("total_orders"),
            sum("Quantity").alias("total_quantity"),
            sum("order_value").alias("total_revenue"),
            avg("order_value").alias("avg_order_value"),
            min("InvoiceDate").alias("first_purchase_date"),
            max("InvoiceDate").alias("last_purchase_date")
        )
    )

    (
        customer_gold_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(customer_gold_table)
    )

    # ---------------------------
    # Daily sales metrics
    # ---------------------------
    daily_gold_df = (
        silver_df
        .withColumn("order_date", to_date(col("InvoiceDate")))
        .withColumn("order_value", col("Quantity") * col("UnitPrice"))
        .groupBy("order_date")
        .agg(
            sum("order_value").alias("daily_revenue"),
            countDistinct("InvoiceNo").alias("daily_orders"),
            sum("Quantity").alias("daily_quantity")
        )
        .orderBy("order_date")
    )

    (
        daily_gold_df
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(daily_gold_table)
    )


if __name__ == "__main__":
    main()
