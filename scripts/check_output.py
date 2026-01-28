from pyspark.sql import SparkSession
import os

# ----------------------------
# Windows environment
# ----------------------------
os.environ["HADOOP_HOME"] = r"C:/hadoop"
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"

# ----------------------------
# Create Spark session
# ----------------------------
spark = SparkSession.builder.appName("CheckBatchOutput").getOrCreate()

# ----------------------------
# File path for processed Parquet
# ----------------------------
processed_path = r"E:/bigdata-ai-pipeline/data/processed/user_spend"

# ----------------------------
# Check if path exists
# ----------------------------
if os.path.exists(processed_path):
    df_check = spark.read.parquet(processed_path)
    print("Schema of processed data:")
    df_check.printSchema()
    print("Preview of aggregated user spend:")
    df_check.show(10)  # show first 10 rows
else:
    print(f"Processed folder does not exist: {processed_path}")
    print("Run the batch job first to generate output.")

# ----------------------------
# Stop Spark session
# ----------------------------
spark.stop()
