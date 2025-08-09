"""Airflow DAG to run dbt models in stg_region folder sequentially."""
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
from airflow.utils.helpers import chain

logger = logging.getLogger(__name__)
DBT_PROJECT_DIR = Path(__file__).resolve().parents[2] / "models" / "dbt"
STG_REGION_DIR = DBT_PROJECT_DIR / "stg_region"
MODEL_NAMES = sorted(p.stem for p in STG_REGION_DIR.glob("*.sql"))
DEFAULT_ARGS = {"owner": "data-eng", "retries": 1, "sla": timedelta(minutes=30)}

def make_dbt_task(model_name: str) -> BashOperator:
    """Create a BashOperator to run a dbt model."""
    return BashOperator(
        task_id=f"dbt_run_{model_name}",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --models {model_name}"
        ),
    )

with DAG(
    dag_id="stg_region",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["region", "staging"],
    default_args=DEFAULT_ARGS,
) as dag:
    logger.info("Configuring stg_region DAG with models: %s", MODEL_NAMES)
    tasks = [make_dbt_task(name) for name in MODEL_NAMES]
    if tasks:
        chain(*tasks)
