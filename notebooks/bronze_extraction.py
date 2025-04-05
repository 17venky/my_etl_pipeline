from etl_pipeline.utils import read_json_file
from etl_pipeline.extractor import extract_fixed_width
from etl_pipeline.validator import apply_validations
from etl_pipeline.writer import write_to_path

layout = read_json_file(spark, FILE_LAYOUT)
df_bronze = extract_fixed_width(spark, INPUT_PATH, layout)
df_validated = apply_validations(df_bronze, layout)
write_to_path(df_validated, BRONZE_PATH)
