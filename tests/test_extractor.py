def test_extraction(spark_session):
    layout = [{"fieldName": "id", "position": 1, "length": 5}]
    path = "tests/sample_fixed_width.txt"
    df = extract_fixed_width(spark_session, path, layout)
    assert df.count() > 0
