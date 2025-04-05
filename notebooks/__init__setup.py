from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName("ETL_Pipeline").getOrCreate()

BASE_PATH = "/Workspace/Repos/17venky/my_etl_pipeline"
CONFIG_PATH = os.path.join(BASE_PATH, "config/pipeline_config.json")
FILE_LAYOUT = os.path.join(BASE_PATH, "file_layout/file_layout.json")
INPUT_PATH = os.path.join(BASE_PATH, "input_data/member_dataset.txt")
BRONZE_PATH = os.path.join(BASE_PATH, "bronze")
SILVER_PATH = os.path.join(BASE_PATH, "silver")
GOLD_PATH = os.path.join(BASE_PATH, "gold")
