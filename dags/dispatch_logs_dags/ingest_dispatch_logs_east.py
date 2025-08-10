"""Ingest dispatch log events from RabbitMQ to Iceberg for east region."""
from datetime import datetime
import logging

from pydantic import BaseModel

from base_ingest import build_ingest_dag

logger = logging.getLogger(__name__)


class DispatchLogEvent(BaseModel):
    event_id: str
    event_ts: datetime
    event_type: str
    dispatch_id: str
    order_id: str
    vehicle_id: str
    status: str
    eta: datetime


COLUMNS = [
    {"name": "event_id", "dataType": "STRING"},
    {"name": "event_ts", "dataType": "TIMESTAMP"},
    {"name": "event_type", "dataType": "STRING"},
    {"name": "dispatch_id", "dataType": "STRING"},
    {"name": "order_id", "dataType": "STRING"},
    {"name": "vehicle_id", "dataType": "STRING"},
    {"name": "status", "dataType": "STRING"},
    {"name": "eta", "dataType": "TIMESTAMP"},
]


dag = build_ingest_dag(
    dag_id="ingest_dispatch_logs_east",
    queue_name="dispatch_logs_east",
    table_fqn="warehouse.fact_dispatch_logs",
    event_model=DispatchLogEvent,
    columns=COLUMNS,
    table_description="Dispatch logs fact table",
    date_field="event_ts",
)
logger.info("Configured ingest_dispatch_logs_east DAG")
