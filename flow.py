import logging
from prefect import flow, task
from prefect.filesystems import S3
from prefect.infrastructure import DockerContainer
from pyspark.sql import SparkSession
from minio import Minio
import tempfile
import os
import glob

from stages.bronze.brewery_api_ingestion import brewery_api_ingestion_flow
from stages.silver.bronze_to_silver import bronze_to_silver
from stages.gold.silver_to_gold import silver_to_gold

@task
def setup_minio_client():
    return Minio(
            os.getenv("MINIO_ENDPOINT", "minio:9000"),
            access_key="minio",
            secret_key="minio123",
            secure=False
        )

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

@flow(name="full-brewery-pipeline")
def full_brewery_pipeline():
    logger = logging.getLogger("Brewing Data... 🍻:")
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(handler)

    logger.info("Starting full brewery pipeline...")

    spark_session = create_spark_session()
    minio_client = setup_minio_client()
    brewery_api_ingestion_flow(minio_client=minio_client)
    bronze_to_silver(spark=spark_session, minio_client=minio_client)
    silver_to_gold(spark=spark_session)
    logger.info("Full brewery pipeline completed.")

if __name__ == "__main__":
    #logging.info("Brewing Data... 🍻:")

    full_brewery_pipeline()

    #spark_session.stop()
