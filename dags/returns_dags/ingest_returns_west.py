from datetime import datetime
import logging
from pydantic import BaseModel

from dags.base_ingest import build_ingest_dag

logger = logging.getLogger(__name__)


class ReturnEvent(BaseModel):
    event_id: str
    event_ts: datetime
    event_type: str
    return_id: str
    order_id: str
    return_ts: datetime
    reason_code: str


columns = [
    {"name": "event_id", "dataType": "STRING"},
    {"name": "event_ts", "dataType": "TIMESTAMP"},
    {"name": "event_type", "dataType": "STRING"},
    {"name": "return_id", "dataType": "STRING"},
    {"name": "order_id", "dataType": "STRING"},
    {"name": "return_ts", "dataType": "TIMESTAMP"},
    {"name": "reason_code", "dataType": "STRING"},
]


dag = build_ingest_dag(
    dag_id="ingest_returns_west",
    queue_name="returns_west",
    table_fqn="warehouse.fact_returns",
    event_model=ReturnEvent,
    columns=columns,
    table_description="Returns fact table",
    date_field="return_ts",
)
logger.info("Configured ingest_returns_west DAG")
