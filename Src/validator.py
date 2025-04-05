def validate_record(record, layout):
    errors = []
    for field in layout:
        fieldName = field["fieldName"]
        val = record.get(fieldName, "")
        vtype = field.get("validationType", "optional")

        if vtype == "required" and not val:
            errors.append((fieldName, field["reactionCode"], field["reactionType"]))

        elif vtype == "regex":
            import re
            pattern = field["validationValues"]
            if not re.match(pattern, val):
                errors.append((fieldName, field["reactionCode"], field["reactionType"]))

        elif vtype == "date":
            from datetime import datetime
            try:
                datetime.strptime(val, "%Y-%m-%d")
            except:
                errors.append((fieldName, field["reactionCode"], field["reactionType"]))

        elif vtype == "list":
            if val not in field["validationValues"]:
                errors.append((fieldName, field["reactionCode"], field["reactionType"]))

        elif vtype == "checkStringLengthBounds":
            bounds = field["validationValues"]
            if not (bounds["min"] <= len(val) <= bounds["max"]):
                errors.append((fieldName, field["reactionCode"], field["reactionType"]))

    return errors

def apply_validations(df, layout, rejected_path):
    validated_rows = []
    rejected_rows = []

    for row in df.collect():
        record = row.asDict()
        errors = validate_record(record, layout)
        
        if not errors or all(e[2] != "Reject Record" for e in errors):
            validated_rows.append(Row(**record))
        else:
            rejected_rows.append(Row(**record))

    valid_df = df.sparkSession.createDataFrame(validated_rows)
    rejected_df = df.sparkSession.createDataFrame(rejected_rows)

    # Save rejected records to the given path
    rejected_df.write.mode("overwrite").json(rejected_path)

    return valid_df
