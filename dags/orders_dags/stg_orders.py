"""Airflow DAG to run the stg_orders dbt model."""
from __future__ import annotations

from pathlib import Path
import logging
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

DBT_PROJECT_DIR = Path(__file__).resolve().parents[2] / "models" / "dbt"
DEFAULT_ARGS = {"owner": "data-eng", "retries": 1, "sla": timedelta(minutes=30)}

with DAG(
    dag_id="stg_orders",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["orders", "staging"],
    default_args=DEFAULT_ARGS,
) as dag:
    logger.info("Configuring stg_orders DAG")
    BashOperator(
        task_id="dbt_run_stg_orders",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --models stg_orders",
    )
