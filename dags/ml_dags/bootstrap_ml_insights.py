"""Bootstrap ML Insights raw Iceberg tables used by dbt sources.

Creates empty Iceberg tables:
- ml_insights.raw_stockout_risk
- ml_insights.raw_demand_forecast

This allows dbt staging models to compile/run even before any ML job
writes data. Idempotent and safe to re-run.
"""
from __future__ import annotations

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import timedelta
import datetime

from base_ingest import _load_iceberg_catalog, _ensure_table_exists, days_ago


DEFAULT_ARGS = {"owner": "data-eng", "retries": 0, "sla": timedelta(minutes=15)}


def create_ml_raw_tables() -> None:
    catalog = _load_iceberg_catalog()
    _ensure_table_exists(
        catalog,
        "ml_insights.raw_stockout_risk",
        [
            {"name": "event_id", "dataType": "STRING"},
            {"name": "event_ts", "dataType": "TIMESTAMP"},
            {"name": "event_type", "dataType": "STRING"},
            {"name": "product_id", "dataType": "STRING"},
            {"name": "risk_score", "dataType": "LONG"},
            {"name": "predicted_date", "dataType": "DATE"},
            {"name": "confidence", "dataType": "LONG"},
        ],
    )
    _ensure_table_exists(
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


with DAG(
    dag_id="bootstrap_ml_insights",
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["ml", "bootstrap"],
    default_args=DEFAULT_ARGS,
) as dag:
    PythonOperator(task_id="create_ml_raw_tables", python_callable=create_ml_raw_tables)

