"""Ingest return events from RabbitMQ to Iceberg for west region."""
from datetime import datetime
import logging

from pydantic import BaseModel

from base_ingest import build_ingest_operator, days_ago
from airflow import DAG

logger = logging.getLogger(__name__)
# sla is defined in build_ingest_operator


class ReturnEvent(BaseModel):
    event_id: str
    event_ts: datetime
    event_type: str
    return_id: str
    order_id: str
    return_ts: datetime
    reason_code: str


COLUMNS = [
    {"name": "event_id", "dataType": "STRING"},
    {"name": "event_ts", "dataType": "TIMESTAMP"},
    {"name": "event_type", "dataType": "STRING"},
    {"name": "return_id", "dataType": "STRING"},
    {"name": "order_id", "dataType": "STRING"},
    {"name": "return_ts", "dataType": "TIMESTAMP"},
    {"name": "reason_code", "dataType": "STRING"},
]


with DAG(
    dag_id="ingest_returns_west",
    schedule="@hourly",
    start_date=days_ago(1),
    catchup=False,
    tags=["ingest"],
    default_args={"owner": "data-eng", "retries": 1},
) as dag:
    build_ingest_operator(
        dag_id="ingest_returns_west",
        queue_name="returns_west",
        table_fqn="warehouse.fact_returns",
        event_model=ReturnEvent,
        columns=COLUMNS,
        table_description="Returns fact table",
        date_field="return_ts",
    )

logger.info("Configured ingest_returns_west DAG")
