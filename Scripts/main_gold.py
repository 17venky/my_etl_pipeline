from pyspark.sql import SparkSession
import os

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Gold Job").config("spark.mongodb.output.uri", "mongodb+srv://venkateshreddy0086:eaShASiYsS1Kakxc@cluster0.iu7jsc0.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0").getOrCreate()

    #BASE_PATH = "/Volumes/workspace/default/venky"
    BASE_PATH = "/home/hemanthkr004/my_project/my_etl_pipeline/input_data"
    SILVER_PATH = os.path.join(BASE_PATH, "silver") 
    GOLD_PATH = os.path.join(BASE_PATH, "gold")

    df_silver = spark.read.parquet(SILVER_PATH)
    df_silver.write.mode("overwrite").format("parquet").save(GOLD_PATH)
    #df_silver.write.format("mongodb").mode("overwrite").save()
    
    spark.stop()
