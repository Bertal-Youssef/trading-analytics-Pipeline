#!/usr/bin/env python3
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime, current_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, LongType

# ---------------------------
# Spark session
# ---------------------------
spark = (
    SparkSession.builder
    .appName("KafkaToBronzeAndClickHouse")
    .config("spark.jars", "/opt/spark/jars/clickhouse-jdbc-0.6.5-shaded.jar")
    .config("spark.driver.extraClassPath", "/opt/spark/jars/*")
    .config("spark.executor.extraClassPath", "/opt/spark/jars/*")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------
# Schema for JSON payload
# ---------------------------
trade_schema = (
    StructType()
    .add("trade_id", StringType())
    .add("symbol", StringType())
    .add("side", StringType())
    .add("price", DoubleType())
    .add("quantity", DoubleType())
    .add("trade_ts", LongType())
)

# ---------------------------
# Read raw events from Kafka
# ---------------------------
print("📡 Connecting to Kafka...")
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "trades_raw")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# ---------------------------
# Prepare two views: RAW + PARSED
# ---------------------------

# Raw view for Bronze
bronze_df = (
    kafka_df.selectExpr(
        "CAST(key AS STRING) AS kafka_key",
        "CAST(value AS STRING) AS raw_value",
        "topic AS kafka_topic",
        "partition AS kafka_partition",
        "offset AS kafka_offset",
        "timestamp AS source_ts"
    )
    .withColumn("ingestion_time", current_timestamp())
)

# Parsed view for ClickHouse
parsed_df = (
    kafka_df
    .select(from_json(col("value").cast("string"), trade_schema).alias("data"))
    .select("data.*")
    .withColumn("trade_ts", from_unixtime(col("trade_ts") / 1000).cast("timestamp"))
)

# ---------------------------
# JDBC configs for ClickHouse
# ---------------------------
clickhouse_cfg = {
    "url": "jdbc:clickhouse://clickhouse:8123/realtime",
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
    "user": "default",
    "password": "",
    "dbtable": "trades_realtime",
    "batchsize": "10000",
    "isolationLevel": "NONE",
    "socket_timeout": "300000",
    "rewriteBatchedStatements": "true"
}

# ---------------------------
# foreachBatch logic
# ---------------------------
def dual_sink_writer(batch_df, batch_id):
    print(f"\n🔹 Processing batch {batch_id}")

    # Split into raw + parsed views
    raw_batch = (
        batch_df.selectExpr(
            "CAST(key AS STRING) AS kafka_key",
            "CAST(value AS STRING) AS raw_value",
            "topic AS kafka_topic",
            "partition AS kafka_partition",
            "offset AS kafka_offset",
            "timestamp AS source_ts"
        ).withColumn("ingestion_time", current_timestamp())
    )

    parsed_batch = (
        batch_df
        .select(from_json(col("value").cast("string"), trade_schema).alias("data"))
        .select("data.*")
        .withColumn("trade_ts", from_unixtime(col("trade_ts") / 1000).cast("timestamp"))
    )

    try:
        # 1️⃣ Append raw to Iceberg (Bronze)
        raw_batch.writeTo("lakehouse.bronze.trades_raw").append()
        print(f"✅ Batch {batch_id}: wrote {raw_batch.count()} rows to Bronze")

        # 2️⃣ Append parsed to ClickHouse
        parsed_batch.write.format("jdbc").options(**clickhouse_cfg).mode("append").save()
        print(f"✅ Batch {batch_id}: wrote {parsed_batch.count()} rows to ClickHouse")

    except Exception as e:
        print(f"❌ Error in batch {batch_id}: {e}")

# ---------------------------
# Start the dual sink stream
# ---------------------------
query = (
    kafka_df.writeStream
    .foreachBatch(dual_sink_writer)
    .outputMode("append")
    .option("checkpointLocation", "/opt/spark-checkpoints/dual-sink")
    .trigger(processingTime="10 seconds")
    .start()
)

print("🚀 Dual-sink streaming started (Kafka → Iceberg + ClickHouse)")
query.awaitTermination()
