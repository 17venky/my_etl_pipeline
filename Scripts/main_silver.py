from pyspark.sql import SparkSession
import os
from Src.transformer import transform_to_silver
from Src.writer import write_to_path
from Src.validator import apply_validations
from Src.utils import read_json_file

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Silver Job") \
        .config("spark.sql.session.timeZone", "UTC") \
        .config("spark.network.timeout", "600s") \
        .config("spark.executor.heartbeatInterval", "60s") \
        .getOrCreate()

    #BASE_PATH = "/Volumes/workspace/default/venky"
    BASE_PATH = "/home/hemanthkr004/my_project/my_etl_pipeline/input_data"
    LAYOUT_PATH = os.path.join(BASE_PATH, "file_layout.json")
    BRONZE_PATH = os.path.join(BASE_PATH, "bronze")
    SILVER_PATH = os.path.join(BASE_PATH, "silver")
    REJECTED_PATH = os.path.join(BASE_PATH, "rejected")
    
    layout = read_json_file(LAYOUT_PATH)
    fields = layout["memberFileLayout"][0]["fields"]

    df_bronze = spark.read.parquet(BRONZE_PATH)
    
    df_valid, df_rejected = apply_validations(df_bronze, fields, REJECTED_PATH)

    df_silver = transform_to_silver(df_valid)

    write_to_path(df_silver, SILVER_PATH)
    write_to_path(df_rejected, REJECTED_PATH)

    spark.stop()
