import os
from Src.extractor import extract_fixed_width
from pyspark.sql import SparkSession

def test_extraction():
    spark = SparkSession.builder.master("local[1]").appName("Test Extraction").getOrCreate()

    input_path = "/home/hemanthkr004/my_project/my_etl_pipeline/input_data/member_dataset.txt"
    
    layout = [
        {"fieldName": "member_id", "position": 1, "length": 15},
        {"fieldName": "storage_info", "position": 16, "length": 20},
        {"fieldName": "source", "position": 36, "length": 15},
        {"fieldName": "code", "position": 51, "length": 10},
    ]

    df = extract_fixed_width(spark, input_path, layout)

    assert df.count() > 0
    assert set(df.columns) == {"member_id", "storage_info", "source", "code"}

    df.show(truncate=False)

    spark.stop()
