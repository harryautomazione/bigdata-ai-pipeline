from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window, sum, count
from pyspark.sql.types import StructType, StringType, DoubleType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import PipelineModel
from pyspark.sql.functions import to_json, struct

# Spark Session
spark = SparkSession.builder \
    .appName("StreamingFraudScoring") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("🔥 Streaming Fraud Scoring Started 🔥")

# Load Trained Model
model_path = "/mnt/e/bigdata-ai-pipeline/models/fraud_model"
model = PipelineModel.load(model_path)

# Define Kafka Schema
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

parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Convert timestamp
parsed_df = parsed_df.withColumn(
    "event_time",
    to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
)

# Window Feature Engineering
feature_df = (
    parsed_df
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        window(col("event_time"), "30 seconds"),
        col("user_id")
    )
    .agg(
        sum("amount").alias("total_spend"),
        count("*").alias("transaction_count")
    )
)

feature_df = feature_df.withColumn(
    "avg_amount",
    col("total_spend") / col("transaction_count")
)

# Apply Model (assembler already inside Pipeline model)
predictions = model.transform(feature_df)

# Select Output Columns 
output_df = predictions.select(
    "user_id",
    "total_spend",
    "transaction_count",
    "avg_amount",
    "prediction",
    "probability"
)

# Convert to JSON for Kafka
kafka_output_df = output_df.select(
    to_json(struct("*")).alias("value")
)

# Output Predictions(on console)
# query = predictions.select(
#  "window",
#  "user_id",
   # "total_spend",
    #"transaction_count",
    #"avg_amount",
    #"probability",
    #"prediction"
#).writeStream \
 #.format("console") \
 #.outputMode("append") \
 #.option("truncate", "false") \
 #.start()

 #(throught kafka)
query = kafka_output_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "fraud_predictions") \
    .option("checkpointLocation", "checkpoints/fraud_scoring") \
    .outputMode("append") \
    .start()

query.awaitTermination()
