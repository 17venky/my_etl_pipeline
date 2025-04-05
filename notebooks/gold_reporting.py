df_silver = spark.read.parquet(SILVER_PATH)
df_silver.write.mode("overwrite").format("delta").save(GOLD_PATH)
