"""Ingest order events from RabbitMQ to Iceberg for north region."""
from datetime import datetime
import logging
import os

from pydantic import BaseModel

from base_ingest import build_ingest_operator, days_ago
from airflow import DAG

logger = logging.getLogger(__name__)
# sla is defined in build_ingest_operator


class OrderEvent(BaseModel):
    event_id: str
    event_ts: datetime
    event_type: str
    order_id: str
    product_id: str
    warehouse_id: str
    order_ts: datetime
    qty: int


COLUMNS = [
    {"name": "event_id", "dataType": "STRING"},
    {"name": "event_ts", "dataType": "TIMESTAMP"},
    {"name": "event_type", "dataType": "STRING"},
    {"name": "order_id", "dataType": "STRING"},
    {"name": "product_id", "dataType": "STRING"},
    {"name": "warehouse_id", "dataType": "STRING"},
    {"name": "order_ts", "dataType": "TIMESTAMP"},
    {"name": "qty", "dataType": "LONG"},
]

with DAG(
    dag_id="ingest_orders_north",
    schedule="@hourly",
    start_date=days_ago(1),
    catchup=False,
    tags=["ingest", "orders", "north"],
    default_args={"owner": "data-eng", "retries": 1},
) as dag:
    build_ingest_operator(
        dag_id="ingest_orders_north",
        queue_name="orders_north",
        table_fqn=f"{os.getenv('ICEBERG_CATALOG', 'minio')}.orders.raw_orders",
        event_model=OrderEvent,
        columns=COLUMNS,
        table_description="Raw orders table",
        date_field="order_ts",
    )

logger.info("Configured ingest_orders_north DAG")

