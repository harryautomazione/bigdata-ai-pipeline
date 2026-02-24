import os
import shutil

os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count
from pyspark.sql.types import DoubleType


def main():
    
    # Create Spark session
    spark = (
    SparkSession.builder
    .appName("BatchUserSpendAggregation")
    .master("local[1]")  
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.sql.warehouse.dir", "file:/C:/spark-warehouse")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "1")
    .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # File paths 
    raw_file_path = "/mnt/e/bigdata-ai-pipeline/data/output"
    processed_path = "/mnt/e/bigdata-ai-pipeline/data/output/user_spend"

    # Clean output folder before writing (Windows-safe)
    if os.path.exists(processed_path):
        shutil.rmtree(processed_path)

    # Read raw parquet
    df = spark.read.parquet(raw_file_path)
    # print("Schema of raw data:")
    # df.printSchema()

    # Basic cleaning: cast amount to numeric and remove nulls/invalid
    clean_df = df.withColumn("amount", col("amount").cast(DoubleType())) \
                .filter(col("amount").isNotNull() & (col("amount") > 0))

    # Aggregation: total spend and transaction count per user
    user_spend = clean_df.groupBy("user_id") \
        .agg(
            sum("amount").alias("total_spend"),
            count("*").alias("transaction_count")
        )
    
    # NOTE:Parquet write causes NativeIO$Windows UnsatisfiedLinkError on Windows local filesystem.
    # This is a known Spark + Hadoop limitation on Windows.
    # Use CSV for local development; Parquet should be used in Linux/WSL/production.
    # ----------------------------
    # Write aggregated data (Parquet)
    # ----------------------------
    user_spend.write.mode("overwrite").parquet(processed_path)

    print(f"Aggregated user spend written to: {processed_path}")

    # Verify output
    df_check = spark.read.parquet(processed_path)
    df_check.show(10, truncate=False)

    # Stop Spark session
    spark.stop()

if __name__ == "__main__":
    main()
