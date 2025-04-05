from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

def extract_fixed_width(spark: SparkSession, file_path: str, layout: list):
    def parse_line(line):
        values = {}
        for field in layout:
            start = field["position"] - 1
            end = start + field["length"]
            values[field["fieldName"]] = line[start:end].strip()
        return values

    raw_rdd = spark.sparkContext.textFile(file_path)
    parsed_rdd = raw_rdd.map(parse_line)
    schema = StructType([StructField(f["fieldName"], StringType(), True) for f in layout])
    return spark.createDataFrame(parsed_rdd.map(lambda x: [x[k] for k in x]), schema)
