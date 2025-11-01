#!/usr/bin/env python3
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower
from pyspark.sql.types import DoubleType

# ----------------------------------------------------------------------
# Spark Session with Iceberg Catalog (ensure configs passed via spark-submit)
# ----------------------------------------------------------------------
spark = (
    SparkSession.builder
        .appName("BronzeToSilverDailyBatch")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
        .config("spark.sql.catalog.lakehouse.uri", "jdbc:postgresql://postgres:5432/iceberg_catalog")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://warehouse/iceberg/")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ----------------------------------------------------------------------
# Read from Bronze
# ----------------------------------------------------------------------
bronze_df = spark.read.format("iceberg").load("lakehouse.bronze.trades_raw")

# ----------------------------------------------------------------------
# Data Cleaning & Standardization
# ----------------------------------------------------------------------
clean_df = (
    bronze_df
        .dropna(subset=["trade_id", "symbol", "side", "price", "quantity"])
        .withColumn("symbol", trim(col("symbol")))
        .withColumn("side", lower(trim(col("side"))))
        .filter(col("side").isin("buy", "sell"))
        .withColumn("price", col("price").cast(DoubleType()))
        .withColumn("quantity", col("quantity").cast(DoubleType()))
        .dropDuplicates(["trade_id"])
)

# ----------------------------------------------------------------------
# Write to Silver (Iceberg)
# ----------------------------------------------------------------------
(
    clean_df.writeTo("lakehouse.silver.trades_clean")
        .option("overwrite-mode", "dynamic")
        .tableProperty("format-version", "2")
        .append()
)

print("✅ Bronze → Silver batch completed successfully.")

spark.stop()
