import os

from workflow.flow import full_brewery_pipeline

from prefect.deployments import Deployment 
from prefect.server.schemas.schedules import CronSchedule  



deployment = Deployment.build_from_flow(
    flow=full_brewery_pipeline,
    name="full-brewery-pipeline-deployment",
    work_queue_name="default",
    tags=["BEES", "brewery", "openbrewerydb"],
    entrypoint="workflow/flow.py:full_brewery_pipeline",
    schedule=(CronSchedule(cron="0 * * * *") if CronSchedule else None),  # hourly if available
)


if __name__ == "__main__":
    output_yaml = os.path.normpath("prefect_data/full_brewery_pipeline-deployment.yaml")
    os.makedirs(os.path.dirname(output_yaml), exist_ok=True)
    deployment.to_yaml(output_yaml)
    print(f"Deployment YAML written to: {output_yaml}")

    deployment.apply()