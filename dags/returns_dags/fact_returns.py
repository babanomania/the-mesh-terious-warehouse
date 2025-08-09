"""Airflow DAG to run the fact_returns dbt model."""
from __future__ import annotations

from pathlib import Path
import logging
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
import datetime
from airflow.utils import timezone

def days_ago(n):
    return timezone.utcnow() - datetime.timedelta(days=n)

logger = logging.getLogger(__name__)

DBT_PROJECT_DIR = Path(__file__).resolve().parents[2] / "models" / "dbt"
DEFAULT_ARGS = {"owner": "data-eng", "retries": 1, "sla": timedelta(minutes=30)}

with DAG(
    dag_id="fact_returns",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["returns", "fact"],
    default_args=DEFAULT_ARGS,
) as dag:
    logger.info("Configuring fact_returns DAG")
    BashOperator(
        task_id="dbt_run_fact_returns",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --models fact_returns",
    )
