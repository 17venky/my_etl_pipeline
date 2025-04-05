def write_parquet(df, path: str):
    df.write.mode("overwrite").parquet(path)
