# Setup Guide

This project aims to demonstrate how a Lakehouse, Data Mesh, and Data Fabric can coexist. The stack is still evolving, but the steps below outline how to get a local environment running and how to verify basic functionality.

## Prerequisites

- [Python](https://www.python.org/) 3.10 or later
- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/)
- GNU Make (optional)

## 1. Clone the Repository

```bash
git clone https://github.com/example/the-mesh-terious-warehouse.git
cd the-mesh-terious-warehouse
```

## 2. Configure Environment Variables

A `.env` file is included in the repository. Review and update any values to match your local environment (ports, credentials, etc.).

## 3. Create a Python Virtual Environment

```bash
python -m venv .venv
source .venv/bin/activate
```

Install project dependencies (a `requirements.txt` file will be added in the future):

```bash
pip install -r requirements.txt  # placeholder for future dependency list
```

## 4. Start Supporting Services

A `docker-compose.yml` file will orchestrate MinIO, RabbitMQ, Airflow, OpenMetadata, and other components. Once this file is available, start the stack with:

```bash
docker compose up -d
```

## 5. Run a Sample Data Generator

With the services running, produce some sample events:

```bash
python ingestion/rabbitmq_producers/orders/produce_orders_north.py --burst 10
```

The script publishes ten order events for the north warehouse to RabbitMQ.

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
