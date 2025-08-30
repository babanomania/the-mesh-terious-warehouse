"""Airflow DAG to predict stockout risks."""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils import timezone

def days_ago(n):
    return timezone.utcnow() - timedelta(days=n)

import os
import uuid

from base_ingest import _load_iceberg_catalog, _ensure_table_exists


def write_stockout_risk(predictions, table_fqn: str | None = None) -> int:
    import pyarrow as pa

    if table_fqn is None:
        table_fqn = "ml_insights.raw_stockout_risk"

    rows = []
    now = datetime.utcnow()
    for p in predictions:
        pd = p["predicted_date"]
        # keep as date if provided
        rows.append(
            {
                "event_id": str(uuid.uuid4()),
                "event_ts": now,
                "event_type": "stockout_risk",
                "product_id": int(p["product_id"]),
                "risk_score": int(p["risk_score"]),
                "predicted_date": pd,
                "confidence": int(p["confidence"]),
                "event_date": now.date(),
            }
        )

    if not rows:
        return 0

    catalog = _load_iceberg_catalog()
    table = _ensure_table_exists(
        catalog,
        table_fqn,
        [
            {"name": "event_id", "dataType": "STRING"},
            {"name": "event_ts", "dataType": "TIMESTAMP"},
            {"name": "event_type", "dataType": "STRING"},
            {"name": "product_id", "dataType": "LONG"},
            {"name": "risk_score", "dataType": "LONG"},
            {"name": "predicted_date", "dataType": "DATE"},
            {"name": "confidence", "dataType": "LONG"},
        ],
    )
    table.append(pa.Table.from_pylist(rows))
    return len(rows)

logger = logging.getLogger(__name__)


def predict_stockout() -> None:
    """Generate a minimal stockout risk prediction and persist it."""
    predictions = [
        {
            "product_id": 1,
            "predicted_date": date.today(),
            "risk_score": 0.0,
            "confidence": 1.0,
        }
    ]
    write_stockout_risk(predictions)
    logger.info(
        "Wrote %d stockout risk predictions", len(predictions)
    )


with DAG(
    dag_id="predict_stockout_risk",
    schedule="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["ml", "risk"],
    default_args={"owner": "data-eng", "retries": 1, "sla": timedelta(minutes=30)},
) as dag:
    logger.info("Configuring predict_stockout_risk DAG")
    PythonOperator(
        task_id="run_stockout_risk_prediction",
        python_callable=predict_stockout,
    )
