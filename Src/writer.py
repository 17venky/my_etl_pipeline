def write_to_path(df, path: str):
    df.write.mode("overwrite").parquet(path)
