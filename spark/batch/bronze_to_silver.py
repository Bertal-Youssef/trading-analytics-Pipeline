#!/usr/bin/env python3
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower
from pyspark.sql.types import DoubleType

spark = (SparkSession.builder
    .appName("BronzeToSilverDailyBatch")
    .getOrCreate())

# Read from Bronze (Iceberg)
bronze_df = spark.read.format("iceberg").load("lakehouse.bronze.trades_raw")

# Basic cleaning
clean_df = (bronze_df
    .dropna(subset=["trade_id", "symbol", "side", "price", "quantity"])
    .withColumn("symbol", trim(col("symbol")))
    .withColumn("side", lower(trim(col("side"))))
    .filter(col("side").isin("buy", "sell"))
    .withColumn("price", col("price").cast(DoubleType()))
    .withColumn("quantity", col("quantity").cast(DoubleType()))
    .dropDuplicates(["trade_id"])
)

# Write to Silver
(clean_df.writeTo("lakehouse.silver.trades_clean")
    .option("overwrite-mode", "dynamic")
    .tableProperty("format-version", "2")
    .append())

print("******** Bronze → Silver batch completed successfully.********")
