import pytest
pytest.importorskip("pyspark") 
from stages.gold.transformations import unify_address_columns, validate_geographic_coordinates, build_valid_url
from pyspark.sql.types import StructType, StructField, StringType


def test_unify_address_columns(spark):
    schema = StructType([
        StructField("address_1", StringType(), True),
        StructField("address_2", StringType(), True),
        StructField("address_3", StringType(), True),
        StructField("street", StringType(), True),
    ])
    df = spark.createDataFrame([
        ("A1", "A2", None, "S"),
        (None, None, None, None),
    ], schema=schema)
    out = unify_address_columns(df)
    rows = [r["full_address"] for r in out.collect()]
    assert rows[0] == "A1, A2, S"
    assert rows[1] is (None or '')


def test_validate_geographic_coordinates(spark):
    df = spark.createDataFrame([{"latitude": 10.0, "longitude": 20.0, "state": "Ohio"},
        {"latitude": 100.0, "longitude": 0.0, "state": "CA"},
        {"latitude": 0.0, "longitude": 200.0, "state": "NY"},
    ])
    out = validate_geographic_coordinates(df)
    rows = out.collect()
    assert len(rows) == 1
    assert rows[0]["country_code"] == "US"
    assert rows[0]["state_code"] == "OH"


def test_build_valid_url(spark):
    df = spark.createDataFrame([
        {"website_url": None},
        {"website_url": "exemplo.com"},
        {"website_url": "http://mybees.com"},
    ])
    out = build_valid_url(df)
    rows = [r["website_url"] for r in out.collect()]
    assert rows[0] is None
    assert rows[1] == "https://exemplo.com"
    assert rows[2] == "https://mybees.com"
