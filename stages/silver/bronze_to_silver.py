
import os
from prefect import flow, task
import pyspark.sql.functions as F
from utils.minio_utils import load_data_from_minio, load_data_to_minio
from .transformations import filter_null_ids_names_and_cities, casting_coordinates_and_postal_code_types

@flow(name="bronze-to-silver")
def bronze_to_silver(spark,minio_client):
    # Load data from bronze layer
    bronze_path = "s3a://brewery-data/bronze/raw_data_breweries.json"
    df_bronze_brewery = load_data_from_minio(spark=spark, minio_client=minio_client, bucket_name="brewery-data", object_name="bronze/raw_data_breweries.json", local_path=bronze_path)

    df_silver_brewery = filter_null_ids_names_and_cities(df_bronze_brewery)
    df_silver_brewery = casting_coordinates_and_postal_code_types(df_silver_brewery)
    df_silver_brewery.show(truncate=False)

    silver_path = "s3a://brewery-data/silver/"
    load_data_to_minio(minio_client, "brewery-data", silver_path, df_silver_brewery)
