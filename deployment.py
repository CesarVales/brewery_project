from prefect.deployment import Deployment
from workflow.flow import full_brewery_pipeline

# Build a Prefect deployment for the full brewery pipeline
deployment = Deployment.build_from_flow(
    flow=full_brewery_pipeline,
    name="full-brewery-pipeline-deployment",
    work_queue_name="default",
    tags=["BEES", "brewery", "data-pipeline"],

    entrypoint="workflow/flow.py:full_brewery_pipeline",
)

if __name__ == "__main__":
    output_yaml = "prefect_data/full_brewery_pipeline-deployment.yaml"
    deployment.to_yaml(output_yaml)
    print(f"Deployment YAML written to: {output_yaml}")

    try:
        deployment.apply()
        print("Deployment applied successfully.")
    except Exception as exc:
        # Do not fail the script if Prefect API isn't reachable/configured
        print(f"Warning: Could not apply deployment automatically: {exc}")