"""Airflow DAG to run the demand forecasting task."""
from __future__ import annotations

import logging
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from ml.forecasting import moving_average

logger = logging.getLogger(__name__)


def run_forecast() -> None:
    """Run a simple moving average forecast and log the result."""
    history = [100, 120, 130, 90, 110, 115, 105]
    forecast = moving_average(history)
    logger.info("Forecast for next period: %s", forecast)


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
