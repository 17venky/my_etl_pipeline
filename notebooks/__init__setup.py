from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.appName("ETL_Pipeline").getOrCreate()

BASE_PATH = "/Workspace/Users/venkateshreddy0086@gmail.com/my_etl_pipeline"
FILE_LAYOUT = os.path.join(BASE_PATH, "input_data/file_layout.json")
CONFIG_PATH = os.path.join(BASE_PATH, "input_data/pipeline_config.json")
INPUT_PATH = os.path.join(BASE_PATH, "input_data/member_dataset.txt")
BRONZE_PATH = os.path.join(BASE_PATH, "bronze")
SILVER_PATH = os.path.join(BASE_PATH, "silver")
GOLD_PATH = os.path.join(BASE_PATH, "gold")
REJECTED_PATH = os.path.join(BASE_PATH, "rejected")

