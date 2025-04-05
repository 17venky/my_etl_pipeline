def read_json_file(spark, path):
    return spark.read.option("multiline", "true").json(path).collect()
