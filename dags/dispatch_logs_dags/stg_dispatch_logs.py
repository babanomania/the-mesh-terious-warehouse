"""Airflow DAG to run the stg_dispatch_logs dbt model."""
from __future__ import annotations

from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

DBT_PROJECT_DIR = Path(__file__).resolve().parents[2] / "models" / "dbt"

with DAG(
    dag_id="stg_dispatch_logs",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["dispatch_logs", "staging"],
) as dag:
    BashOperator(
        task_id="dbt_run_stg_dispatch_logs",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --models stg_dispatch_logs",
    )
