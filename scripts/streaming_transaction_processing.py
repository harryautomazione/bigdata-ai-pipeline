from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType
from pyspark.sql.functions import col, to_timestamp, sum
from pyspark.sql.functions import window, sum, count, avg

# Create Spark session 
spark = SparkSession.builder \
    .appName("KafkaToParquetStreaming") \
    .getOrCreate()

print("🔥 STREAMING FILE EXECUTED 🔥")

spark.sparkContext.setLogLevel("WARN")

# Schema
schema = StructType() \
    .add("event_id", StringType()) \
    .add("user_id", StringType()) \
    .add("amount", DoubleType()) \
    .add("device", StringType()) \
    .add("location", StringType()) \
    .add("event_time", StringType())

# Read from Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transaction_events") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Convert event_time to TIMESTAMP
parsed_df = parsed_df.withColumn(
    "event_time",
    to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
)

parsed_df.select("event_time").printSchema()

# Aggregation: total spend per user
agg_df = (
    parsed_df
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("user_id")
    )
    .agg(
        sum("amount").alias("total_spend"),
        count("*").alias("transaction_count"),
        avg("amount").alias("avg_amount")
    )
)

# Flatten window column
feature_df = agg_df.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("user_id"),
    col("total_spend"),
    col("transaction_count"),
    col("avg_amount")
)

# this is for debuging
    # .format("console") \
    # .option("truncate", "false") \
    # .option("checkpointLocation", "../checkpoints/transactions_console") \
# Write stream to parquet
#query = parsed_df.writeStream \
    #.format("parquet") \
    #.outputMode("append") \
    #.option("path", "../data/output") \
    #.option("checkpointLocation", "../checkpoints/transactions_parquet") \
    #.start()

# Write aggregated output
# query = agg_df.writeStream \
    #.format("parquet") \
    #.outputMode("append") \
    #.option("path", "../data/output/user_spend") \
    #.option("checkpointLocation", "../checkpoints/user_spend_agg") \
    #.start()

# Write as parquet store
query = feature_df.writeStream \
    .format("parquet") \
    .outputMode("append") \
    .option("path", "../data/feature_store") \
    .option("checkpointLocation", "../checkpoints/feature_store") \
    .start()


print("🚀 Streaming query started")
query.awaitTermination()
