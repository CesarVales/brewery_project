from prefect import flow,get_run_logger

from stages.bronze.brewery_api_ingestion import brewery_api_ingestion_flow
from stages.silver.bronze_to_silver import bronze_to_silver
from stages.gold.silver_to_gold import silver_to_gold
from utils.setup_utils import setup_minio_client, create_spark_session

@flow(name="full-brewery-pipeline")
def full_brewery_pipeline():
    logger = get_run_logger()
    logger.info("Starting full brewery pipeline...")
    logger.info("Brewing Data...🍺")

    spark_session = create_spark_session()
    minio_client = setup_minio_client()
    brewery_api_ingestion_flow(minio_client=minio_client)
    bronze_to_silver(spark=spark_session, minio_client=minio_client)
    silver_to_gold(spark=spark_session)

    logger.info("Data Brewed!!! 🍻🍻🍻")

if __name__ == "__main__":
    full_brewery_pipeline()

