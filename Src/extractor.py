from pyspark.sql import SparkSession
from pyspark.sql.functions import substring, trim

def extract_fixed_width(spark: SparkSession, file_path: str, layout: list):
    df = spark.read.text(file_path)
    for field in layout:
        field_name = field["fieldName"]
        start = field["position"]
        length = field["length"]
        df = df.withColumn(field_name, trim(substring("value", start, length)))
    return df.drop("value")