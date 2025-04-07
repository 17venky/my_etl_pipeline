import pandas as pd
from pyspark.sql.functions import col

def apply_validations(df, layout, rejected_path):
    def validate_rows(pdf_iter):
        for pdf in pdf_iter:
            pdf["_rejected"] = False
            pdf["_rejectedReasons"] = [[] for _ in range(len(pdf))]

            for field in layout:
                fieldName = field["fieldName"]
                vtype = field.get("validationType", "optional").lower()
                reaction = field.get("reactionType", "").lower()
                reason = field.get("reactionCode", "")

                # Default mask
                mask = pd.Series([False] * len(pdf))

                if vtype == "required":
                    mask = pdf[fieldName].isnull() | (pdf[fieldName].astype(str).str.strip() == "")

                elif vtype == "regex":
                    pattern = field["validationValues"]
                    mask = ~pdf[fieldName].astype(str).str.match(pattern, na=False)

                elif vtype == "date":
                    mask = pd.to_datetime(pdf[fieldName], errors='coerce').isna()

                elif vtype == "list":
                    allowed = field["validationValues"]
                    mask = ~pdf[fieldName].isin(allowed)

                elif vtype == "checkstringlengthbounds":
                    bounds = field["validationValues"]
                    lengths = pdf[fieldName].astype(str).str.len()
                    mask = ~lengths.between(bounds["min"], bounds["max"])

                # Only apply rejections if reactionType is reject
                if reaction == "reject record":
                    pdf["_rejected"] |= mask
                    for idx in mask[mask].index:
                        pdf.at[idx, "_rejectedReasons"].append(reason)

            yield pdf

    from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType

    schema_with_rejected = StructType([
        StructField("memberId", StringType(), True),
        StructField("groupId", StringType(), True),
        StructField("familyId", StringType(), True),
        StructField("firstName", StringType(), True),
        StructField("lastName", StringType(), True),
        StructField("relationship", StringType(), True),
        StructField("sexCode", StringType(), True),
        StructField("dateOfBirth", StringType(), True),
        StructField("socialSecurityNumber", StringType(), True),
        StructField("address1", StringType(), True),
        StructField("country", StringType(), True),
        StructField("familyType", StringType(), True),
        StructField("phoneNumber", StringType(), True),
        StructField("effectiveDate", StringType(), True),
        StructField("terminationDate", StringType(), True),
        StructField("plan", StringType(), True),
        StructField("_rejected", BooleanType(), True),
        StructField("_rejectedReasons", ArrayType(StringType()), True),
    ])

    validated_df = df.mapInPandas(validate_rows, schema=schema_with_rejected)

    valid_df = validated_df.filter(col("_rejected") == False).drop("_rejected", "_rejectedReasons")
    rejected_df = validated_df.filter(validated_df["_rejected"] == True).drop("_rejected")

    if rejected_df.count() > 0:
        rejected_df.write.mode("overwrite").json(rejected_path)

    return valid_df, rejected_df

