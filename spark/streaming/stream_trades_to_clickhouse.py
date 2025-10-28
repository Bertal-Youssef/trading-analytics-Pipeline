#!/usr/bin/env python3
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, from_unixtime
from pyspark.sql.types import StructType, StringType, DoubleType, LongType

# Create Spark session with explicit JAR configuration
spark = SparkSession.builder \
    .appName("KafkaToClickHouse") \
    .config("spark.jars", "/opt/spark/jars/clickhouse-jdbc-0.6.5-shaded.jar") \
    .config("spark.driver.extraClassPath", "/opt/spark/jars/*") \
    .config("spark.executor.extraClassPath", "/opt/spark/jars/*") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema for trade data
schema = StructType() \
    .add("trade_id", StringType()) \
    .add("symbol", StringType()) \
    .add("side", StringType()) \
    .add("price", DoubleType()) \
    .add("quantity", DoubleType()) \
    .add("trade_ts", LongType())

# Read from Kafka topic
print("📡 Connecting to Kafka...")
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "trades_raw") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse JSON messages
parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Convert timestamp from milliseconds to timestamp
parsed_df = parsed_df.withColumn(
    "trade_ts", 
    from_unixtime(col("trade_ts") / 1000).cast("timestamp")
)

# ClickHouse connection properties
clickhouse_properties = {
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

# Function to write each microbatch to ClickHouse
def write_to_clickhouse(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"⏭️  Batch {batch_id} is empty, skipping...")
        return
    
    record_count = batch_df.count()
    print(f"📝 Writing batch {batch_id} with {record_count} records")
    
    try:
        # Show sample data for debugging
        print(f"Sample data from batch {batch_id}:")
        batch_df.show(5, truncate=False)
        
        # Write to ClickHouse
        batch_df.write \
            .format("jdbc") \
            .options(**clickhouse_properties) \
            .mode("append") \
            .save()
        
        print(f"✅ Batch {batch_id} written successfully ({record_count} records)")
        
    except Exception as e:
        print(f"❌ Error writing batch {batch_id}: {type(e).__name__}")
        print(f"   Error message: {str(e)}")
        
        # Print more detailed error information
        import traceback
        traceback.print_exc()
        
        # Don't raise - allows stream to continue processing
        # If you want to fail the entire job on error, uncomment:
        # raise

# Start streaming query
print("🚀 Starting streaming query...")
query = parsed_df.writeStream \
    .foreachBatch(write_to_clickhouse) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/clickhouse-checkpoint") \
    .trigger(processingTime="10 seconds") \
    .start()

print("✨ Streaming from Kafka to ClickHouse started...")
print("📊 Waiting for data...")
query.awaitTermination()