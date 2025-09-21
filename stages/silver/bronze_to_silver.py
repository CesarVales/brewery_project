
import os
from prefect import flow, task
from pyspark.sql import SparkSession
from minio import Minio

@task
def load_data_from_bronze(spark, minio_client, bucket_name, object_name, local_path):
    if not minio_client.bucket_exists(bucket_name):
        raise ValueError(f"Bucket {bucket_name} does not exist.")
    minio_client.fget_object(bucket_name, object_name, local_path)
    return spark.read.json(local_path)


def filter_null_ids_names_cities(df):
    return df.filter(df.id.isNotNull() & df.name.isNotNull() & df.city.isNotNull())

def load_data_to_silver(minio_client, bucket_name, path,df):
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
    return df.write.mode("overwrite").parquet(path)


@flow(name="bronze-to-silver")
def bronze_to_silver(spark,minio_client):
    # Load data from bronze layer
    bronze_path = "s3a://brewery-data/bronze/raw_data_breweries.json"
    df_bronze_brewery = load_data_from_bronze(spark=spark, minio_client=minio_client, bucket_name="brewery-data", object_name="bronze/raw_data_breweries.json", local_path=bronze_path)

    # Apply filters to create silver DataFrame
    df_silver_brewery = filter_null_ids_names_cities(df_bronze_brewery)
    df_silver_brewery.show(truncate=False)
    # Save silver data back to MinIO
    silver_path = "s3a://brewery-data/silver/"
    load_data_to_silver(minio_client, "brewery-data", silver_path, df_silver_brewery)
    
