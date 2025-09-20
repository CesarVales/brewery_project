import logging
from prefect import flow, task
from prefect.filesystems import S3
from prefect.infrastructure import DockerContainer
from pyspark.sql import SparkSession
from minio import Minio
import tempfile
import os
import glob

from stages.brewery_api_ingestion import brewery_api_ingestion_flow
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
            .config("spark.ui.enabled", "true")

    builder = builder.master(spark_master)

    return builder.getOrCreate()

@flow(name="spark-minio-integration")
def spark_minio_flow():
    # Setup clients
    minio_client = setup_minio_client()
    spark = create_spark_session()
    
    print('*'*55)
    data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
    df = spark.createDataFrame(data, ["Name", "Value"])
    
    # Create bucket without s3a:// prefix
    bucket_name = "test-bucket"
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
    
    # Write data and find the actual file created by Spark
    df.coalesce(1).write.mode("overwrite").json("/tmp/data/")
    
    # en: glob lib enable regex for path
    # pt: lib do glob possibilita regex para path
    json_files = glob.glob("/tmp/data/*.json")
    if json_files:
        json_file = json_files[0]
        print(f"Uploading JSON file: {json_file}")
        minio_client.fput_object(bucket_name, "data.json", json_file)
    else:
        print("No JSON files found in /tmp/data/")

    print("Flow completed successfully!")
    spark.stop()

if __name__ == "__main__":
    #logging.info("Brewing Data... 🍻:")
    #spark_session = create_spark_session()

    brewery_api_ingestion_flow()


    #spark_session.stop()
