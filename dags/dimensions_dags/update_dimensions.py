"""Build-only DAG: runs dbt dimension models.

This DAG only executes `dbt run --select dimensions`.
Use `update_dimensions_metadata` to handle OpenMetadata registration and lineage.
"""
from __future__ import annotations

import os
import datetime
from datetime import timedelta
import logging
from pathlib import Path

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils import timezone


def days_ago(n: int) -> datetime.datetime:
    return timezone.utcnow() - datetime.timedelta(days=n)


DBT_PROJECT_DIR = Path(__file__).resolve().parents[1] / "models" / "dbt"
DEFAULT_ARGS = {"owner": "data-eng", "retries": 1, "sla": timedelta(minutes=30)}


with DAG(
    dag_id="update_dimensions",
    schedule="@hourly",
    start_date=days_ago(1),
    catchup=False,
    tags=["dimensions", "dbt"],
    default_args=DEFAULT_ARGS,
) as dag:
    BashOperator(
        task_id="dbt_build_dimensions",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select path:dimensions",
    )
