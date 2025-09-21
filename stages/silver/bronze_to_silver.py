
import os
from prefect import flow
import pyspark.sql.functions as F
from .transformations import filter_null_ids_names_and_cities, casting_coordinates_and_postal_code_types

@flow(name="bronze-to-silver")
def bronze_to_silver(spark,minio_client):
    # Load data from bronze layer
    bronze_path = "s3a://brewery-data/bronze/raw_data_breweries.json"
    df_bronze_brewery = spark.read.json(bronze_path)

    df_silver_brewery = filter_null_ids_names_and_cities(df_bronze_brewery)
    df_silver_brewery = casting_coordinates_and_postal_code_types(df_silver_brewery)
    df_silver_brewery.show(truncate=False)

    silver_path = "s3a://brewery-data/silver/"
    df_silver_brewery.write.mode("overwrite").parquet(silver_path)
