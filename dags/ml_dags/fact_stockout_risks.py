"""Airflow DAG to run the fact_stockout_risks dbt model."""
from __future__ import annotations

from pathlib import Path
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

logger = logging.getLogger(__name__)

DBT_PROJECT_DIR = Path(__file__).resolve().parents[2] / "models" / "dbt"

with DAG(
    dag_id="fact_stockout_risks",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["ml", "fact"],
) as dag:
    logger.info("Configuring fact_stockout_risks DAG")
    BashOperator(
        task_id="dbt_run_fact_stockout_risks",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --models fact_stockout_risks",
    )
