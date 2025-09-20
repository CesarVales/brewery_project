from prefect import flow, task
from prefect.filesystems import S3
from prefect.infrastructure import DockerContainer
from pyspark.sql import SparkSession
from minio import Minio
import tempfile
import os

@task
def setup_minio_client():
    return Minio(
        "minio:9000",
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
    
    # Create a sample dataframe
    data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
    df = spark.createDataFrame(data, ["Name", "Value"])
    df.show()
    
    print("Flow completed successfully!")
    spark.stop()

if __name__ == "__main__":
    spark_minio_flow()