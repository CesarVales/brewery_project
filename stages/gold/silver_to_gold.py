
import os
from prefect import flow, task
from pyspark.sql import SparkSession
from minio import Minio
from pyspark.sql import functions as F

@task
def load_data_from_bronze(minio_client, bucket_name, object_name, local_path):
    if not minio_client.bucket_exists(bucket_name):
        raise ValueError(f"Bucket {bucket_name} does not exist.")
    minio_client.fget_object(bucket_name, object_name, local_path)


def filter_null_ids_names_cities(df):
    return df.filter(df.id.isNotNull() & df.name.isNotNull() & df.city.isNotNull())

@flow(name="silver-to-gold")
def silver_to_gold(minio_client):
    spark = create_spark_session()
    silver_path = "s3a://brewery-data/silver/brewery_silver.parquet"
    df_silver_brewery = spark.read.parquet(silver_path)

    df_gold_brewery = df_silver_brewery.withColumn(
        "full_address",
        F.concat_ws(", ",
            F.col("address_1"),
            F.col("address_2"),
            F.col("address_3"),
            F.col("street")
        )
    )
    gold_path = "s3a://brewery-data/gold/brewery_gold.parquet"
    df_gold_brewery.write.mode("overwrite").parquet(gold_path)




@task
def create_spark_session():

    spark_master = os.getenv("SPARK_MASTER_URL", "local[*]")

    builder = SparkSession.builder \
        .appName("PrefectSparkMinio") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")


    if spark_master.startswith("local"):
        builder = builder \
            .config("spark.driver.bindAddress", "0.0.0.0") \
            .config("spark.ui.enabled", "false")

    builder = builder.master(spark_master)

    return builder.getOrCreate()
