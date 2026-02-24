import os
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline

def main():

    spark = (
        SparkSession.builder
        .appName("FraudModelTraining")
        .master("local[1]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    # Read feature store
    feature_path = "/mnt/e/bigdata-ai-pipeline/data/feature_store"

    df = spark.read.parquet(feature_path)

    print("Feature Schema:")
    df.printSchema()

    # Create fraud label (demo logic)
    df = df.withColumn(
        "label",
        when(col("total_spend") > 10000, 1).otherwise(0)
    )

    print("Training Data Preview:")
    df.show(5)

    # Select features
    feature_columns = [
        "total_spend",
        "transaction_count",
        "avg_amount"
    ]

    assembler = VectorAssembler(
        inputCols=feature_columns,
        outputCol="features"
    )

    lr = LogisticRegression(
        featuresCol="features",
        labelCol="label"
    )

    pipeline = Pipeline(stages=[assembler, lr])

    model = pipeline.fit(df)

    # Save model
    model_path = "/mnt/e/bigdata-ai-pipeline/models/fraud_model"

    model.write().overwrite().save(model_path)

    print("Model trained and saved successfully at:")
    print(model_path)

    spark.stop()


if __name__ == "__main__":
    main()
