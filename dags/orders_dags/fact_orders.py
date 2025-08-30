"""Airflow DAG to run the fact_orders dbt model."""
from __future__ import annotations

from pathlib import Path
import logging
from datetime import timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
import datetime
from airflow.utils import timezone

def days_ago(n):
    return timezone.utcnow() - datetime.timedelta(days=n)

logger = logging.getLogger(__name__)

DBT_PROJECT_DIR = Path(__file__).resolve().parents[1] / "models" / "dbt"
# Include an SLA for this DAG to ensure timely fact table builds
DEFAULT_ARGS = {"owner": "data-eng", "retries": 1, "sla": timedelta(minutes=30)}

with DAG(
    dag_id="fact_orders",
    schedule="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["orders", "fact"],
    default_args=DEFAULT_ARGS,
) as dag:
    logger.info("Configuring fact_orders DAG")
    wait_for_staging = ExternalTaskSensor(
        task_id="wait_for_stg_orders",
        external_dag_id="stg_orders",
        poke_interval=60,
        timeout=60 * 60,
    )

    dbt_run_fact_orders = BashOperator(
        task_id="dbt_run_fact_orders",
        # Run the fact model along with any upstream dependencies
        # so that staging models like ``stg_orders`` are built before the
        # ``fact_orders`` model executes. Without the ``+`` selector dbt only
        # runs the specified model, which resulted in failures when the
        # required staging table was missing.
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select +fact_orders",
    )

    wait_for_staging >> dbt_run_fact_orders
