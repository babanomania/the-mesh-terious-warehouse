"""Ingest inventory movement events from RabbitMQ to Iceberg for south region."""
from datetime import datetime
import logging

from pydantic import BaseModel

from base_ingest import build_ingest_dag

logger = logging.getLogger(__name__)


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
    {"name": "delta_qty", "dataType": "INT"},
    {"name": "source_type", "dataType": "STRING"},
]


dag = build_ingest_dag(
    dag_id="ingest_inventory_south",
    queue_name="inventory_south",
    table_fqn="warehouse.fact_inventory_movements",
    event_model=InventoryEvent,
    columns=COLUMNS,
    table_description="Inventory movements fact table",
    date_field="event_ts",
)
logger.info("Configured ingest_inventory_south DAG")
