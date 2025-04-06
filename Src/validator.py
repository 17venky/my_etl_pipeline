import pandas as pd

def apply_validations(df, layout, rejected_path):
    def validate_rows(pdf_iter):
        for pdf in pdf_iter:
            pdf["_rejected"] = False

            for field in layout:
                fieldName = field["fieldName"]
                vtype = field.get("validationType", "optional")
                reaction = field.get("reactionType", "")

                if vtype == "required":
                    mask = pdf[fieldName].isnull() | (pdf[fieldName].astype(str).str.strip() == "")
                    if reaction == "Reject Record":
                        pdf["_rejected"] |= mask

                elif vtype == "regex":
                    import re
                    pattern = field["validationValues"]
                    mask = ~pdf[fieldName].astype(str).str.match(pattern)
                    if reaction == "Reject Record":
                        pdf["_rejected"] |= mask

                elif vtype == "date":
                    from datetime import datetime
                    mask = pd.to_datetime(pdf[fieldName], errors='coerce').isna()
                    if reaction == "Reject Record":
                        pdf["_rejected"] |= mask

                elif vtype == "list":
                    allowed = field["validationValues"]
                    mask = ~pdf[fieldName].isin(allowed)
                    if reaction == "Reject Record":
                        pdf["_rejected"] |= mask

                elif vtype == "checkStringLengthBounds":
                    bounds = field["validationValues"]
                    mask = ~pdf[fieldName].astype(str).str.len().between(bounds["min"], bounds["max"])
                    if reaction == "Reject Record":
                        pdf["_rejected"] |= mask

            yield pdf

    validated_df = df.mapInPandas(validate_rows, schema=df.schema.add("_rejected", "boolean"))

    # Split DataFrames
    valid_df = validated_df.filter(validated_df["_rejected"] == False).drop("_rejected")
    rejected_df = validated_df.filter(validated_df["_rejected"] == True).drop("_rejected")

    # Save rejected records
    if rejected_df.count() > 0:
        rejected_df.write.mode("overwrite").json(rejected_path)

    return valid_df
