from pyspark.sql import SparkSession
import os
from etl_pipeline.transformer import transform_to_silver
from etl_pipeline.writer import write_to_path

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Silver Job").getOrCreate()

    BASE_PATH = "/Workspace/Users/venkateshreddy0086@gmail.com/my_etl_pipeline"
    BRONZE_PATH = os.path.join(BASE_PATH, "bronze")
    SILVER_PATH = os.path.join(BASE_PATH, "silver")

    df_bronze = spark.read.parquet(BRONZE_PATH)
    df_silver = transform_to_silver(df_bronze)

    write_to_path(df_silver, SILVER_PATH)

    spark.stop()
