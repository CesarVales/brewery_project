import requests

from prefect import flow, get_run_logger
from prefect.filesystems import S3
from prefect.infrastructure import DockerContainer
from pyspark.sql import SparkSession
import os
from minio import Minio
import glob
from logging import Logger

from dotenv import load_dotenv
load_dotenv()

def setup_minio_client():
    return Minio(
        os.getenv("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minio"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minio123"),
        secure=False
    )

def api_data_fetch():
    api_url = os.getenv("API_URL", "https://api.openbrewerydb.org/v1/breweries")
    response = requests.get(api_url)
    
    retries = 2
    while retries > 0:
        try:
            response.raise_for_status()
            if response.status_code == 200:
                return response.json()
        except requests.RequestException as e:
            logger = get_run_logger()
            logger.error(f"Error fetching API data: {e}")
            retries -= 1
            if retries == 0:
                logger.error("Max retries reached. Aborting.")
                return None

    logger.info(f"Fetched data from API: {api_url} with status code {response.status_code}")
    return None

def upload_to_minio(minio_client, bucket_name, object_name, file_path):
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)
    minio_client.fput_object(bucket_name, object_name, file_path)

@flow(name="brewery-api-ingestion")
def brewery_api_ingestion_flow():
    
    logger = get_run_logger()

    # Fetch data from the API
    data = api_data_fetch()
    
    # Save data to a temporary JSON file
    temp_file_path = "/tmp/raw_data_breweries.json"
    os.makedirs(os.path.dirname(temp_file_path), exist_ok=True)
    with open(temp_file_path, 'w') as f:
        import json
        json.dump(data, f)
    logger.info(f"Saved API data to temporary file: {temp_file_path}")

    minio_client = setup_minio_client()
    
    # Upload to MinIO
    bucket_name = "brewery-data"
    object_name = "raw_data_breweries.json"
    upload_to_minio(minio_client, bucket_name, object_name, temp_file_path)

    logger.info(f"Uploaded {object_name} to bucket {bucket_name} in MinIO.")


    # Clean up temporary file
    #os.remove(temp_file_path)