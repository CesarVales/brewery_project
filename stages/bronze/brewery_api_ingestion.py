
import os
import json
import requests

from prefect import flow, get_run_logger
from dotenv import load_dotenv
load_dotenv()


def api_data_fetch():
    api_url = os.getenv("API_URL")
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

def create_and_save_temp_file(data, temp_file_path="/tmp/raw_data_breweries.json"):
    os.makedirs(os.path.dirname(temp_file_path), exist_ok=True)
    with open(temp_file_path, 'w') as f:
        json.dump(data, f)
    return temp_file_path

@flow(name="brewery-api-ingestion")
def brewery_api_ingestion_flow(minio_client):
    
    logger = get_run_logger()

    # Fetch data from the API
    data = api_data_fetch()
    
    temp_file_path = create_and_save_temp_file(data)

    # Upload to MinIO
    bucket_name = "brewery-data"
    object_name = "bronze/raw_data_breweries.json"
    upload_to_minio(minio_client, bucket_name, object_name, temp_file_path)

    logger.info(f"Uploaded {object_name} to bucket {bucket_name} in MinIO.")

    # Clean up temporary file
    os.remove(temp_file_path)