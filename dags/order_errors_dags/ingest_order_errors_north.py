"""Ingest order error events from RabbitMQ to Iceberg for north region."""
from datetime import datetime
import logging

from pydantic import BaseModel, Field

from base_ingest import build_ingest_operator, days_ago
from airflow import DAG

logger = logging.getLogger(__name__)


class OrderErrorEvent(BaseModel):
    event_id: str
    event_ts: datetime
    event_type: str
    error_id: str
    order_id: str
    error_code: str
    error_ts: datetime = Field(alias="detected_ts")


COLUMNS = [
    {"name": "event_id", "dataType": "STRING"},
    {"name": "event_ts", "dataType": "TIMESTAMP"},
    {"name": "event_type", "dataType": "STRING"},
    {"name": "error_id", "dataType": "STRING"},
    {"name": "order_id", "dataType": "STRING"},
    {"name": "error_code", "dataType": "STRING"},
    {"name": "error_ts", "dataType": "TIMESTAMP"},
]


with DAG(
    dag_id="ingest_order_errors_north",
    schedule="@hourly",
    start_date=days_ago(1),
    catchup=False,
    tags=["ingest", "order_errors", "north"],
    default_args={"owner": "data-eng", "retries": 1},
) as dag:
    build_ingest_operator(
        dag_id="ingest_order_errors_north",
        queue_name="errors_north",
        table_fqn="order_errors.raw_order_errors",
        event_model=OrderErrorEvent,
        columns=COLUMNS,
        table_description="Raw order error events table",
        date_field="error_ts",
    )

logger.info("Configured ingest_order_errors_north DAG")

