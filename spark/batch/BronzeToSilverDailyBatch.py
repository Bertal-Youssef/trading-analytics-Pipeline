#!/usr/bin/env python3
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# ----------------------------------------------------------------------
# Spark Session with Iceberg Catalog (ensure configs passed via spark-submit)
# ----------------------------------------------------------------------
spark = (
    SparkSession.builder
        .appName("BronzeToSilverDailyBatch")
        .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.jdbc.JdbcCatalog")
        .config("spark.sql.catalog.lakehouse.uri", "jdbc:postgresql://postgres:5432/iceberg_catalog")
        .config("spark.sql.catalog.lakehouse.jdbc.user", "airflow")
        .config("spark.sql.catalog.lakehouse.jdbc.password", "airflow")
        .config("spark.sql.catalog.lakehouse.warehouse", "s3a://warehouse/iceberg/")
        .config("spark.sql.catalog.lakehouse.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalogImplementation", "in-memory")
        .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ----------------------------------------------------------------------
# Define the schema for parsing the raw JSON from Bronze
# ----------------------------------------------------------------------
trade_schema = StructType([
    StructField("trade_id", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", DoubleType(), True),
    StructField("side", StringType(), True),  # Changed from trade_type to side
    StructField("timestamp", TimestampType(), True),
])

print("📖 Reading from Bronze layer: lakehouse.bronze.trades_raw")

# ----------------------------------------------------------------------
# Read from Bronze (contains raw Kafka data with raw_value column)
# ----------------------------------------------------------------------
bronze_df = spark.read.format("iceberg").load("lakehouse.bronze.trades_raw") 
#pay attention !!!!!!!!!!!! each time he loads the old trades we should read just the newest ones to the silver

print(f"✅ Bronze records found: {bronze_df.count()}")
print("🔍 Bronze schema:")
bronze_df.printSchema()

# ----------------------------------------------------------------------
# Parse the JSON from raw_value column
# ----------------------------------------------------------------------
parsed_df = bronze_df \
    .withColumn("parsed", from_json(col("raw_value"), trade_schema)) \
    .select(
        col("parsed.trade_id").alias("trade_id"),
        col("parsed.symbol").alias("symbol"),
        col("parsed.price").alias("price"),
        col("parsed.quantity").alias("quantity"),
        col("parsed.side").alias("side"),
        col("parsed.timestamp").alias("trade_ts"),
        col("source_ts"),
        col("ingestion_time")
    )

# ----------------------------------------------------------------------
# Data Cleaning & Standardization
# ----------------------------------------------------------------------
clean_df = (
    parsed_df
        .dropna(subset=["trade_id", "symbol", "side", "price", "quantity"])
        .withColumn("symbol", trim(col("symbol")))
        .withColumn("side", lower(trim(col("side"))))
        .filter(col("side").isin("buy", "sell"))
        .withColumn("price", col("price").cast(DoubleType()))
        .withColumn("quantity", col("quantity").cast(DoubleType()))
        .dropDuplicates(["trade_id"])
)

print(f"✅ Clean records after deduplication: {clean_df.count()}")

# ----------------------------------------------------------------------
# Write to Silver (Iceberg)
# ----------------------------------------------------------------------
print("💾 Writing to Silver layer: lakehouse.silver.trades_clean")

(
    clean_df.writeTo("lakehouse.silver.trades_clean")
        .using("iceberg")
        .option("overwrite-mode", "dynamic")
        .tableProperty("format-version", "2")
        .createOrReplace()  # Changed from append() to createOrReplace() for first run !!!!!!!!pay attention!!!!!
)

print("✅ Bronze → Silver batch completed successfully.")

spark.stop()