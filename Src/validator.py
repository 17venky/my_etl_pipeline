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
