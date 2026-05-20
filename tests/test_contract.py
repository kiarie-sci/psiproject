import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from models.contract import validate_contract

@pytest.fixture(scope="session")
def spark():
    s = SparkSession.builder.master("local[2]").appName("pytest-spark").getOrCreate()
    yield s
    s.stop()

def test_validate_contract_pass(spark):
    rows = [Row(dataElement_uid="A", dataElement_name="Name", orgUnit_uid="O", facility_uid="F", period="202401", year_month="2024-01", typed_value=1.0, is_late_reported=False)]
    df = spark.createDataFrame(rows)
    assert validate_contract(df) is True

def test_validate_contract_missing_col(spark):
    rows = [Row(dataElement_uid="A", orgUnit_uid="O", facility_uid="F", period="202401", year_month="2024-01", typed_value=1.0, is_late_reported=False)]
    df = spark.createDataFrame(rows)
    with pytest.raises(AssertionError):
        validate_contract(df)
