
import os
from prefect import flow, task
from pyspark.sql import SparkSession
from minio import Minio

@task
def load_data_from_bronze(minio_client, bucket_name, object_name, local_path):
    if not minio_client.bucket_exists(bucket_name):
        raise ValueError(f"Bucket {bucket_name} does not exist.")
    minio_client.fget_object(bucket_name, object_name, local_path)


def filter_null_ids_names_cities(df):
    return df.filter(df.id.isNotNull() & df.name.isNotNull() & df.city.isNotNull())

@flow(name="bronze-to-silver")
def bronze_to_silver(minio_client):
    spark = create_spark_session()
    # Load data from bronze layer
    bronze_path = "brewery-data/raw_data_breweries.json"
    load_data_from_bronze(minio_client, "brewery-data", "raw_data_breweries.json", bronze_path)
    df_bronze_brewery = spark.read.json(bronze_path)

    # Apply filters to create silver DataFrame
    df_silver_brewery = filter_null_ids_names_cities(df_bronze_brewery)
    df_silver_brewery.show(truncate=False)
    # Save silver data back to MinIO
    silver_path = "s3a://brewery-data/silver/brewery_silver.parquet"
    df_silver_brewery.write.mode("overwrite").parquet(silver_path)



@task
def create_spark_session():
    # Allow overriding the Spark master via environment variable for flexibility.
    # Default to local[*] which works inside a single container (avoids cluster-mode LiveListenerBus issues).
    spark_master = os.getenv("SPARK_MASTER_URL", "local[*]")

    builder = SparkSession.builder \
        .appName("PrefectSparkMinio") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # When running in local mode, ensure Spark's listener bus and driver host are configured to bind correctly
    # This helps avoid errors like 'LiveListenerBus is stopped' when the driver can't initialize listeners.
    if spark_master.startswith("local"):
        builder = builder \
            .config("spark.driver.bindAddress", "0.0.0.0") \
            .config("spark.ui.enabled", "false")

    builder = builder.master(spark_master)

    return builder.getOrCreate()
