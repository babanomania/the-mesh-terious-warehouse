# Setup Guide

This project aims to demonstrate how a Lakehouse, Data Mesh, and Data Fabric can coexist. The stack is still evolving, but the steps below outline how to get a local environment running and how to verify basic functionality.

## Prerequisites

- [Python](https://www.python.org/) 3.10 or later
- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/)
- GNU Make (optional)

## 1. Clone the Repository

```bash
git clone https://github.com/babanomania/the-mesh-terious-warehouse.git
cd the-mesh-terious-warehouse
```

## 2. Configure Environment Variables

Create a `.env` file in the repo root (if not present) and review/update values to match your local environment (ports, credentials, etc.).

Key variables for the local stack:

- `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`: MinIO credentials
- `ICEBERG_REST_URI` / `ICEBERG_WAREHOUSE`: Iceberg REST catalog and warehouse path
- `RABBITMQ_USER` / `RABBITMQ_PASSWORD`: RabbitMQ credentials
- `OPENMETADATA_HOSTPORT` / `OPENMETADATA_JWT_TOKEN`: OpenMetadata API access (if used by DAGs)
- `_PIP_ADDITIONAL_REQUIREMENTS`: Space-separated pip packages installed in Airflow containers at startup (see step 4)

## 3. Create a Python Virtual Environment

```bash
python -m venv .venv
source .venv/bin/activate
```

Install project dependencies for the ingestion scripts:

```bash
pip install -r ingestion/requirements.txt
```

## 4. Start Supporting Services

The `docker-compose.yml` orchestrates MinIO, RabbitMQ, Iceberg REST, Airflow (Celery, Redis, Postgres), OpenMetadata, and Superset. Start the stack with:

```bash
docker compose up -d
```

> Airflow Python deps: To run ingestion DAGs that use RabbitMQ and Iceberg, ensure the containers install `pika`, `pyiceberg`, and `pyarrow`. The simplest path is to set in your `.env`:
>
> `_PIP_ADDITIONAL_REQUIREMENTS="pika pyiceberg pyarrow"`
>
> Alternatively, build a custom Airflow image with these packages baked in.

> First-run users: Default admin users for Airflow and Superset are created automatically using credentials from your `.env` file (defaults shown below).

Once the containers are running, access each service's UI at:

- RabbitMQ Management: http://localhost:15672 (`guest`/`guest`)
- MinIO Console: http://localhost:9001 (`minioadmin`/`minioadmin`)
- Iceberg REST API: http://localhost:8181 (no auth)
- Airflow Console: http://localhost:8080 (`admin`/`admin`)
- OpenMetadata: http://localhost:8585 (`admin@open-metadata.org`/`admin`)
- Superset: http://localhost:8088 (`admin`/`admin`)

## 5. Run Data Generators

With the services running, you can launch one or more mock data generators using
the provided CLI:

```bash
python ingestion/start_generators.py --mode burst --burst-count 10 --domains orders
```

This command starts all generator scripts under the `orders` domain and emits
ten events from each script. Use `--dry-run` to preview commands without
execution and `--domains all` to run every available domain.

### `start_generators.py` options

| Flag | Description |
| ---- | ----------- |
| `--mode {live,burst,replay}` | Generation mode (`live` default) |
| `--interval N` | Seconds between events in live mode (default `10`) |
| `--burst-count N` | Events to emit per script in burst mode (default `100`) |
| `--replay-path PATH` | CSV file to replay in replay mode |
| `--domains d1,d2` | Comma-separated list of domains (`all` default) |
| `--dry-run` | Show commands without executing |

### Examples

```bash
# Live mode for two domains with faster interval
python ingestion/start_generators.py --mode live --interval 5 --domains orders,returns

# Replay mode using events from a CSV for all domains
python ingestion/start_generators.py --mode replay --replay-path ./data/orders.csv --domains all
```

## 6. Run Project Checks

Run the test suite with `pytest`:

```bash
pytest -q
```

The repo includes tests for generator schemas, DAG syntax, Iceberg schema, metadata bootstrapping, and basic ML utilities.

## 7. Shut Down

When finished, stop the containers (once `docker-compose.yml` exists):

```bash
docker compose down
```

Deactivate the virtual environment:

```bash
deactivate
```

---

This document will evolve as the project adds infrastructure, dependencies, and automated tests.
