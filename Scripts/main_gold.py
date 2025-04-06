from pyspark.sql import SparkSession
import os

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Gold Job").getOrCreate()

    BASE_PATH = "/Volumes/workspace/default/venky"
    SILVER_PATH = os.path.join(BASE_PATH, "silver")
    GOLD_PATH = os.path.join(BASE_PATH, "gold")

    df_silver = spark.read.parquet(SILVER_PATH)
    df_silver.write.mode("overwrite").format("delta").save(GOLD_PATH)

    spark.stop()
