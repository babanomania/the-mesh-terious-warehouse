"""Airflow DAG to run the stg_forecast_demand dbt model."""
from __future__ import annotations

from pathlib import Path
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

DBT_PROJECT_DIR = Path(__file__).resolve().parents[2] / "models" / "dbt"

with DAG(
    dag_id="stg_forecast_demand",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["ml", "staging"],
) as dag:
    logger.info("Configuring stg_forecast_demand DAG")
    BashOperator(
        task_id="dbt_run_stg_forecast_demand",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --models stg_forecast_demand",
    )
