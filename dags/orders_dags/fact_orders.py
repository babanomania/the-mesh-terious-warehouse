"""Airflow DAG to run the fact_orders dbt model."""
from __future__ import annotations

from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

DBT_PROJECT_DIR = Path(__file__).resolve().parents[2] / "models" / "dbt"

with DAG(
    dag_id="fact_orders",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["orders", "fact"],
) as dag:
    BashOperator(
        task_id="dbt_run_fact_orders",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --models fact_orders",
    )
