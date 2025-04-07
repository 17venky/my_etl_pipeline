import pytest
from pyspark.sql import SparkSession
from etl_pipeline.validator import apply_validations

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("Validation Test") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_apply_validations_required(spark, tmp_path):
    sample_data = [{
        "memberId": "", "groupId": "G1", "familyId": "F1", "firstName": "John",
        "lastName": "Doe", "relationship": "Self", "sexCode": "M", "dateOfBirth": "1990-01-01",
        "socialSecurityNumber": "123456789", "address1": "123 Main", "country": "US",
        "familyType": "Nuclear", "phoneNumber": "1234567890", "effectiveDate": "2020-01-01",
        "terminationDate": "2022-01-01", "plan": "Basic"
    }]
    df = spark.createDataFrame(sample_data)

    validation_layout = [{
        "fieldName": "memberId",
        "validationType": "required",
        "reactionCode": "ID MISSING",
        "reactionType": "Reject Record"
    }]

    rejected_path = tmp_path / "rejected"

    # Run validation
    valid_df, rejected_df = apply_validations(df, validation_layout, str(rejected_path))

    # Assertions
    assert valid_df.count() == 0
    assert rejected_df.count() == 1
    rejection_reasons = rejected_df.select("_rejectedReasons").collect()[0]["_rejectedReasons"]
    assert "ID MISSING" in rejection_reasons
