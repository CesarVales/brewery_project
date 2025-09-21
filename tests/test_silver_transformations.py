import pytest
pytest.importorskip("pyspark")
import pyspark.sql.functions as F
from stages.silver.transformations import filter_null_ids_names_and_cities, casting_coordinates_and_postal_code_types


def test_filter_null_ids_names_and_cities(spark):
    data = [
        {"id": "1", "name": "A", "city": "X"},
        {"id": None, "name": "B", "city": "X"},
        {"id": "3", "name": None, "city": "X"},
        {"id": "4", "name": "D", "city": None},
        {"id": "5", "name": "E", "city": "Y"},
    ]
    df = spark.createDataFrame(data)
    out = filter_null_ids_names_and_cities(df)
    assert out.count() == 2


def test_casting_coordinates_and_postal_code_types(spark):
    data = [
        {"id": "1", "name": "A", "city": "X", "latitude": "10.1", "longitude": "20.2", "postal_code": 123},
        {"id": "2", "name": "B", "city": "Y", "latitude": None, "longitude": None, "postal_code": "7"},
    ]
    df = spark.createDataFrame(data)
    out = casting_coordinates_and_postal_code_types(df)
    assert dict(out.dtypes)["latitude"] == "double"
    assert dict(out.dtypes)["longitude"] == "double"
    assert dict(out.dtypes)["postal_code"] == "string"
    row = out.orderBy(F.col("id").asc()).collect()
    assert row[0]["postal_code"] == "00123"
    assert row[1]["postal_code"] == "00007"
