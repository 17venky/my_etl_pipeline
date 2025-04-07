from pyspark.sql.functions import col
from pyspark.sql.types import DateType, StringType

def transform_to_silver(df):
    # Step 1: Cast columns to proper types
    df = df.withColumn("dateOfBirth", col("dateOfBirth").cast(DateType())) \
           .withColumn("effectiveDate", col("effectiveDate").cast(DateType())) \
           .withColumn("terminationDate", col("terminationDate").cast(DateType())) \
           .withColumn("familyType", col("familyType").cast(StringType())) \
           .withColumn("relationship", col("relationship").cast(StringType()))

    # Step 2: Drop rows with nulls in required fields
    required_columns = [
        "memberId", "groupId", "familyId", "firstName", "lastName",
        "relationship", "sexCode", "dateOfBirth", "socialSecurityNumber",
        "address1", "country", "familyType", "phoneNumber",
        "effectiveDate", "terminationDate", "plan"
    ]
    return df.dropna(subset=required_columns)

