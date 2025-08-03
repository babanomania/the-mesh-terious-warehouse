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

QUEUE_NAME = "orders_north"
TABLE_FQN = "warehouse.fact_orders"
CATALOG_NAME = os.getenv("ICEBERG_CATALOG", "local")


class OrderEvent(BaseModel):
    event_id: str
    event_ts: datetime
    event_type: str
    order_id: str
    product_id: str
    warehouse_id: str
    order_ts: datetime
    qty: int


def register_with_openmetadata(rows_count: int) -> None:
    server_config = OpenMetadataServerConfig(
        hostPort=os.getenv("OPENMETADATA_HOSTPORT", "http://localhost:8585/api"),
        authProvider="no-auth",
    )
    metadata = OpenMetadata(server_config)
    request = CreateTableRequest(
        name="fact_orders",
        tableType="Regular",
        columns=[
            {"name": "event_id", "dataType": "STRING"},
            {"name": "event_ts", "dataType": "TIMESTAMP"},
            {"name": "event_type", "dataType": "STRING"},
            {"name": "order_id", "dataType": "STRING"},
            {"name": "product_id", "dataType": "STRING"},
            {"name": "warehouse_id", "dataType": "STRING"},
            {"name": "order_ts", "dataType": "TIMESTAMP"},
            {"name": "qty", "dataType": "INT"},
            {"name": "event_date", "dataType": "DATE"},
        ],
        owner=EntityReference(id="00000000-0000-0000-0000-000000000000", type="user"),
        description="Orders fact table",
    )
    metadata.create_or_update(request)
    logging.info("Registered %s rows to OpenMetadata", rows_count)


def consume_and_write() -> None:
    credentials = pika.PlainCredentials(
        os.getenv("RABBITMQ_USER", "guest"), os.getenv("RABBITMQ_PASSWORD", "guest")
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
            event = OrderEvent(**payload)
            record = event.dict()
            record["event_date"] = event.order_ts.date().isoformat()
            rows.append(record)
            channel.basic_ack(method_frame.delivery_tag)
        except ValidationError as exc:
            logging.error("Validation error: %s", exc)
            channel.basic_nack(method_frame.delivery_tag, requeue=False)

    channel.close()
    connection.close()

    if not rows:
        logging.info("No messages consumed")
        return

    catalog = load_catalog(CATALOG_NAME)
    table: Table = catalog.load_table(TABLE_FQN)
    table.append(pa.Table.from_pylist(rows))
    register_with_openmetadata(len(rows))


def build_dag() -> DAG:
    with DAG(
        dag_id="ingest_orders_north",
        schedule_interval="@hourly",
        start_date=days_ago(1),
        catchup=False,
        default_args={"owner": "data-eng", "retries": 1},
    ) as dag:
        consume = PythonOperator(
            task_id="consume_orders",
            python_callable=consume_and_write,
            sla=timedelta(minutes=15),
        )
    return dag


dag = build_dag()
