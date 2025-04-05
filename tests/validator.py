from etl_pipeline.validator import validate_record

def test_required_validation():
    layout = [{
        "fieldName": "id",
        "validationType": "required",
        "reactionCode": "ID MISSING",
        "reactionType": "Reject Record"
    }]
    record = {"id": ""}
    errors = validate_record(record, layout)
    assert len(errors) == 1
