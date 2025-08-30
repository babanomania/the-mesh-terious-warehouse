"""Ingest inventory movement events from RabbitMQ to Iceberg for west region."""
from datetime import datetime
import logging

from pydantic import BaseModel

from base_ingest import build_ingest_operator, days_ago
from airflow import DAG

logger = logging.getLogger(__name__)
# sla is defined in build_ingest_operator


class InventoryEvent(BaseModel):
    event_id: str
    event_ts: datetime
    event_type: str
    movement_id: str
    product_id: str
    delta_qty: int
    source_type: str


COLUMNS = [
    {"name": "event_id", "dataType": "STRING"},
    {"name": "event_ts", "dataType": "TIMESTAMP"},
    {"name": "event_type", "dataType": "STRING"},
    {"name": "movement_id", "dataType": "STRING"},
    {"name": "product_id", "dataType": "STRING"},
    {"name": "delta_qty", "dataType": "LONG"},
    {"name": "source_type", "dataType": "STRING"},
]


with DAG(
    dag_id="ingest_inventory_west",
    schedule="@hourly",
    start_date=days_ago(1),
    catchup=False,
    tags=["ingest", "inventory", "west"],
    default_args={"owner": "data-eng", "retries": 1},
) as dag:
    build_ingest_operator(
        dag_id="ingest_inventory_west",
        queue_name="inventory_west",
        table_fqn="inventory.raw_inventory_movements",
        event_model=InventoryEvent,
        columns=COLUMNS,
        table_description="Raw inventory movements table",
        date_field="event_ts",
    )

logger.info("Configured ingest_inventory_west DAG")
