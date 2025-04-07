from pyspark.sql import SparkSession
import os
from Src.utils import read_json_file
from Src.extractor import extract_fixed_width
#from Src.validator import apply_validations
from Src.writer import write_to_path

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Bronze Job").getOrCreate()

    #BASE_PATH = "/Volumes/workspace/default/venky"
    BASE_PATH = "/home/hemanthkr004/my_project/my_etl_pipeline/input_data"
    INPUT_PATH = os.path.join(BASE_PATH, "member_dataset.txt")
    LAYOUT_PATH = os.path.join(BASE_PATH, "file_layout.json")
    #REJECTED_PATH = os.path.join(BASE_PATH, "rejected")
    BRONZE_PATH = os.path.join(BASE_PATH, "bronze")

    layout = read_json_file(LAYOUT_PATH)
    
    fields = layout["memberFileLayout"][0]["fields"]

    df = extract_fixed_width(spark, INPUT_PATH, fields)
    
    write_to_path(df, BRONZE_PATH)

    spark.stop()
