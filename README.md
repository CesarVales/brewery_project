# Brewery Project – Run Locally with Docker

Data pipeline using Prefect 2, Spark, and MinIO. This guide shows how to bring everything up with Docker and run the flow locally in a reproducible way.

## Overview
- Orchestration: Prefect Server (UI at `http://localhost:4200`) and a Prefect agent
- Object storage: MinIO (console at `http://localhost:9001` – user `minio`, password `minio123`)
- Processing: Spark
- Prefect database: Postgres (via Docker)

Main files used:
- `docker-compose.yaml` – defines the services
- `deployment.py` – creates/applies the Prefect deployment for the flow `workflow/flow.py:full_brewery_pipeline`

## Prerequisites
- Docker Desktop (Compose v2)
- Port 4200 free (Prefect UI), and ports 9000/9001 free (MinIO)

You don't need Python installed locally: all commands below are executed inside the `prefect` container.

## Bring up the stack
1) Build the images (includes custom images for Spark and Prefect)
```powershell
docker compose build
```

2) Start the services
```powershell
docker compose up -d
```

3) Quick access
- Prefect UI: `http://localhost:4200`
- MinIO Console: `http://localhost:9001` (user: `minio`, pass: `minio123`)

## Start the Prefect agent
The agent must be running and listening to the `default` queue (configured in compose via `PREFECT_API_URL`).

- Interactive (attaches to your terminal):
```powershell
docker exec -it prefect bash -lc "prefect agent start -q default"
```

- Background (does not block your terminal):
```powershell
docker exec -d prefect bash -lc "prefect agent start -q default"
```

You can monitor runs from the Prefect UI or by attaching to the agent container logs.

## Create and apply the deployment
The file `deployment.py` generates a YAML at `prefect_data/full_brewery_pipeline-deployment.yaml` and attempts to apply the deployment automatically.

Run inside the `prefect` container:
```powershell
docker exec -it prefect bash -lc "python deployment.py"
```

If the automatic apply fails (e.g., Prefect API is still starting), apply the YAML manually:
```powershell
docker exec -it prefect bash -lc "prefect deployment apply prefect_data/full_brewery_pipeline-deployment.yaml"
```

Optional: run the flow directly without deployment (for a quick smoke test):
```powershell
docker exec -it prefect bash -lc "python -c \"from workflow.flow import full_brewery_pipeline; full_brewery_pipeline()\""
```

Note: the deployment already includes a schedule (cron) to run every hour.

## Run the flow on demand
List and trigger the deployment manually:
```powershell
docker exec -it prefect bash -lc "prefect deployment ls"
docker exec -it prefect bash -lc "prefect deployment run 'full-brewery-pipeline/full-brewery-pipeline-deployment'"
```

## Stop and clean up
- Stop services:
```powershell
docker compose down
```

- Stop and remove volumes (MinIO and Postgres data will be deleted):
```powershell
docker compose down -v
```

## Troubleshooting
- Prefect UI doesn't open: ensure port 4200 is free and the `prefect-server` container is healthy (`docker ps`).
- Agent isn't picking up work: confirm it's running and listening to the `default` queue.
  - Tip: run again `docker exec -it prefect bash -lc "prefect agent start -q default"`.
- Prefect import errors (in your editor): make sure you're executing commands inside the `prefect` container, which already has the correct pinned dependencies (`requirements.txt`).
- After changing `requirements.txt`: rebuild the `prefect` image to ensure updated libs are installed:
```powershell
docker compose build --no-cache prefect
docker compose up -d
```
- MinIO default credentials: user `minio`, password `minio123` (set in `docker-compose.yaml`).

## Flow structure
The main flow is `full_brewery_pipeline` in `workflow/flow.py`. The deployment uses the entrypoint:
```
workflow/flow.py:full_brewery_pipeline
```
Transformations live in `stages/` (bronze, silver, gold) and utilities in `utils/`.

---

With this, anyone can bring up the stack, apply the deployment, and run the pipeline locally using Docker.
