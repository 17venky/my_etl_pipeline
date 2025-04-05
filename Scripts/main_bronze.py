from pyspark.sql import SparkSession
import os
from etl_pipeline.utils import read_json_file
from etl_pipeline.extractor import extract_fixed_width
from etl_pipeline.validator import apply_validations
from etl_pipeline.writer import write_to_path

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Bronze Job").getOrCreate()

    BASE_PATH = "/Workspace/Users/venkateshreddy0086@gmail.com/my_etl_pipeline"
    INPUT_PATH = os.path.join(BASE_PATH, "input_data/member_dataset.txt")
    LAYOUT_PATH = os.path.join(BASE_PATH, "input_data/file_layout.json")
    REJECTED_PATH = os.path.join(BASE_PATH, "rejected")
    BRONZE_PATH = os.path.join(BASE_PATH, "bronze")

    layout = read_json_file(spark, LAYOUT_PATH)
    df = extract_fixed_width(spark, INPUT_PATH, layout)
    df_valid = apply_validations(df, layout, REJECTED_PATH)
    write_to_path(df_valid, BRONZE_PATH)

    spark.stop()
