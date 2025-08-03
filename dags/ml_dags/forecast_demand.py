"""Airflow DAG to run the demand forecasting task."""
from __future__ import annotations

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def run_forecast() -> None:
    """Placeholder callable for demand forecasting logic."""
    print("Executing demand forecast placeholder")


with DAG(
    dag_id="forecast_demand",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["ml", "forecast"],
) as dag:
    PythonOperator(
        task_id="run_demand_forecast",
        python_callable=run_forecast,
    )
