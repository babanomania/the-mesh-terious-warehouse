import json
import logging
import os
from datetime import datetime, timedelta

import pika
import pyarrow as pa
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pydantic import BaseModel, ValidationError
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table

from metadata.generated.schema.api.data.createTable import CreateTableRequest
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.ingestion.ometa.openmetadata import OpenMetadata
from metadata.ingestion.ometa.config import OpenMetadataServerConfig

logger = logging.getLogger(__name__)

QUEUE_NAME = "dispatch_logs_north"
TABLE_FQN = "warehouse.fact_dispatch_logs"
CATALOG_NAME = os.getenv("ICEBERG_CATALOG", "local")


class DispatchLogEvent(BaseModel):
    event_id: str
    event_ts: datetime
    event_type: str
    dispatch_id: str
    order_id: str
    vehicle_id: str
    status: str
    eta: datetime


def register_with_openmetadata(rows_count: int) -> None:
    server_config = OpenMetadataServerConfig(
        hostPort=os.getenv("OPENMETADATA_HOSTPORT", "http://localhost:8585/api"),
        authProvider="no-auth",
    )
    metadata = OpenMetadata(server_config)
    request = CreateTableRequest(
        name="fact_dispatch_logs",
        tableType="Regular",
        columns=[
            {"name": "event_id", "dataType": "STRING"},
            {"name": "event_ts", "dataType": "TIMESTAMP"},
            {"name": "event_type", "dataType": "STRING"},
            {"name": "dispatch_id", "dataType": "STRING"},
            {"name": "order_id", "dataType": "STRING"},
            {"name": "vehicle_id", "dataType": "STRING"},
            {"name": "status", "dataType": "STRING"},
            {"name": "eta", "dataType": "TIMESTAMP"},
            {"name": "event_date", "dataType": "DATE"},
        ],
        owner=EntityReference(id="00000000-0000-0000-0000-000000000000", type="user"),
        description="Dispatch logs fact table",
    )
    metadata.create_or_update(request)
    logger.info("Registered %s rows to OpenMetadata", rows_count)


def consume_and_write() -> None:
    credentials = pika.PlainCredentials(
        os.getenv("RABBITMQ_USER", "guest"),
        os.getenv("RABBITMQ_PASSWORD", "guest"),
    )
    parameters = pika.ConnectionParameters(
        host=os.getenv("RABBITMQ_HOST", "localhost"),
        port=int(os.getenv("RABBITMQ_PORT", "5672")),
        credentials=credentials,
    )
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    rows = []
    for method_frame, properties, body in channel.consume(QUEUE_NAME, inactivity_timeout=1):
        if body is None:
            break
        try:
            payload = json.loads(body)
            event = DispatchLogEvent(**payload)
            record = event.dict()
            record["event_date"] = event.event_ts.date().isoformat()
            rows.append(record)
            channel.basic_ack(method_frame.delivery_tag)
        except ValidationError as exc:
            logger.error("Validation error: %s", exc)
            channel.basic_nack(method_frame.delivery_tag, requeue=False)

    channel.close()
    connection.close()

    if not rows:
        logger.info("No messages consumed")
        return

    catalog = load_catalog(CATALOG_NAME)
    table: Table = catalog.load_table(TABLE_FQN)
    table.append(pa.Table.from_pylist(rows))
    register_with_openmetadata(len(rows))


def build_dag() -> DAG:
    with DAG(
        dag_id="ingest_dispatch_logs_north",
        schedule_interval="@hourly",
        start_date=days_ago(1),
        catchup=False,
        default_args={"owner": "data-eng", "retries": 1},
    ) as dag:
        PythonOperator(
            task_id="consume_dispatch_logs",
            python_callable=consume_and_write,
            sla=timedelta(minutes=15),
        )
    return dag


dag = build_dag()
logger.info("Configured ingest_dispatch_logs_north DAG")
