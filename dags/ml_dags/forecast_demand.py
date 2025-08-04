"""Airflow DAG to run the demand forecasting task."""
from __future__ import annotations

import logging
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)


def run_forecast() -> None:
    """Placeholder callable for demand forecasting logic."""
    logger.info("Executing demand forecast placeholder")


with DAG(
    dag_id="forecast_demand",
    schedule_interval="@daily",
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
