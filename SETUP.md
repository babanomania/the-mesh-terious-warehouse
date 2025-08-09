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

A `.env` file is included in the repository. Review and update any values to match your local environment (ports, credentials, etc.).

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

A `docker-compose.yml` file will orchestrate MinIO, RabbitMQ, Airflow, OpenMetadata, and other components. Once this file is available, start the stack with:

```bash
docker compose up -d
```

> **Note:** The Airflow image now includes a system compiler (`g++`) so any Python dependencies requiring C++ compilation (e.g., Iceberg, OpenMetadata ingestion) can build successfully at container build time.
> **Note:** The Airflow container will automatically install the RabbitMQ client library (`pika`) and the Iceberg Python client (`pyiceberg`) on startup so that ingestion DAGs can import them.

> **Note:** On first startup, default admin users for Airflow and Superset are created automatically using credentials from your `.env` file (defaults shown below).

Once the containers are running, access each service's UI at:

- RabbitMQ Management: http://localhost:15672 (`guest`/`guest`)
- MinIO Console: http://localhost:9001 (`minioadmin`/`minioadmin`)
- Airflow Console: http://localhost:8080 (`admin`/`admin`)
- OpenMetadata: http://localhost:8585 (`admin`/`admin`)
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

Basic tests can be run with `pytest`:

```bash
pytest
```

At this stage there are no unit tests, so the command should report `0 tests`.

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
