from etl_pipeline.transformer import transform_to_silver
from etl_pipeline.writer import write_to_path

df_bronze = spark.read.parquet(BRONZE_PATH)
df_silver = transform_to_silver(df_bronze)
write_to_path(df_silver, SILVER_PATH)
