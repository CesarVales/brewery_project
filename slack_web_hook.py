import requests
from prefect import task
from jobs.utils import logger

@task
def send_automatisch_notification():
    workflow_id = "failed_etl_notification"  
    automatisch_url = f"http://localhost:4200/api/v1/trigger/{workflow_id}"

    payload = {"message": """
    !!!!ALERT!!!!! - Pipeline Failed
    The ETL pipeline has encountered an error during execution. Check the logs for more details.
    """}

    try:
        response = requests.post(automatisch_url, json=payload)

        if response.status_code in (200, 201, 202):
            logger.info("Notification sent successfully to Automatisch!")
        else:
            logger.error(f"Failed: {response.status_code} - {response.text}")

    except Exception as e:
        logger.error(f"Exception while sending notification: {str(e)}")