from pyspark.sql import SparkSession
import os
from Src.transformer import transform_to_silver
from Src.writer import write_to_path

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Silver Job") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.network.timeout", "600s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .getOrCreate()

    BASE_PATH = "/Volumes/workspace/default/venky"
    BRONZE_PATH = os.path.join(BASE_PATH, "bronze")
    SILVER_PATH = os.path.join(BASE_PATH, "silver")

    df_bronze = spark.read.parquet(BRONZE_PATH)
    df_silver = transform_to_silver(df_bronze)

    write_to_path(df_silver, SILVER_PATH)

    spark.stop()