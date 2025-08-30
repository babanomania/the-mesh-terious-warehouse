"""Airflow DAG to run the demand forecasting task and write to Iceberg."""
from __future__ import annotations

import logging
from datetime import timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import datetime
from airflow.utils import timezone
import uuid
from datetime import datetime as dt

from base_ingest import _load_iceberg_catalog, _ensure_table_exists

def days_ago(n):
    return timezone.utcnow() - datetime.timedelta(days=n)

def moving_average(history):
    values = list(history)
    if not values:
        raise ValueError("history cannot be empty")
    return sum(values) / len(values)

logger = logging.getLogger(__name__)


def write_demand_forecast(forecast_qty: float) -> int:
    """Write a single forecast row to ml_insights.raw_demand_forecast via PyIceberg."""
    import pyarrow as pa

    catalog = _load_iceberg_catalog()
    table = _ensure_table_exists(
        catalog,
        "ml_insights.raw_demand_forecast",
        [
            {"name": "event_id", "dataType": "STRING"},
            {"name": "event_ts", "dataType": "TIMESTAMP"},
            {"name": "event_type", "dataType": "STRING"},
            {"name": "product_id", "dataType": "STRING"},
            {"name": "region", "dataType": "STRING"},
            {"name": "forecast_ts", "dataType": "TIMESTAMP"},
            {"name": "forecast_qty", "dataType": "LONG"},
        ],
    )
    now = dt.utcnow()
    row = {
        "event_id": str(uuid.uuid4()),
        "event_ts": now,
        "event_type": "forecast_demand",
        "product_id": "1",
        "region": "global",
        "forecast_ts": now,
        "forecast_qty": int(round(forecast_qty)),
        "event_date": now.date(),
    }
    table.append(pa.Table.from_pylist([row]))
    return 1


def run_forecast() -> None:
    """Run a simple moving average forecast and log the result."""
    history = [100, 120, 130, 90, 110, 115, 105]
    forecast = moving_average(history)
    logger.info("Forecast for next period: %s", forecast)
    try:
        write_demand_forecast(forecast)
        logger.info("Wrote demand forecast to Iceberg")
    except Exception as exc:
        logger.warning("Failed to write demand forecast: %s", exc)


with DAG(
    dag_id="forecast_demand",
    schedule="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["ml", "forecast"],
    default_args={"owner": "data-eng", "retries": 1, "sla": timedelta(minutes=30)},
) as dag:
    logger.info("Configuring forecast_demand DAG")
    PythonOperator(
        task_id="run_demand_forecast",
        python_callable=run_forecast,
    )
