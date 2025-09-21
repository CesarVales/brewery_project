
import os
import requests
from prefect import flow, get_run_logger
from utils.minio_utils import upload_to_minio_from_memory

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




@flow(name="brewery-api-ingestion")
def brewery_api_ingestion_flow(minio_client):
    
    logger = get_run_logger()

    data = api_data_fetch()
    
    bucket_name = "brewery-data"
    object_name = "bronze/raw_data_breweries.json"

    upload_to_minio_from_memory(minio_client, bucket_name, object_name, data)

    logger.info(f"Uploaded {object_name} to bucket {bucket_name} in MinIO.")
    logger.info("Brewery API Ingestion Completed.")