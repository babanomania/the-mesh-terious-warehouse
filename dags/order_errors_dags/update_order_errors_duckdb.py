"""Create DuckDB views for order_errors module once (idempotent)."""
from __future__ import annotations

import os
import re
import datetime
from datetime import timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils import timezone


def days_ago(n: int) -> datetime.datetime:
    return timezone.utcnow() - datetime.timedelta(days=n)


DEFAULT_ARGS = {"owner": "data-eng", "retries": 0, "sla": timedelta(minutes=10)}


def create_views() -> None:
    import duckdb

    def strip_scheme(endpoint: str) -> str:
        return re.sub(r"^https?://", "", endpoint or "")

    db_path = os.getenv("DUCKDB_PATH", "/data/warehouse.duckdb")
    con = duckdb.connect(database=db_path)

    minio_endpoint = strip_scheme(os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
    access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")

    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute("SET s3_url_style='path';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET unsafe_enable_version_guessing=true;")
    con.execute(f"SET s3_endpoint='{minio_endpoint}';")
    con.execute(f"SET s3_access_key_id='{access_key}';")
    con.execute(f"SET s3_secret_access_key='{secret_key}';")
    # No catalog attach; use iceberg_scan paths directly

    con.execute("CREATE SCHEMA IF NOT EXISTS order_errors;")
    con.execute(
        "CREATE OR REPLACE VIEW order_errors.fact_order_errors AS "
        "select * from iceberg_scan('" + os.getenv("ICEBERG_WAREHOUSE", "s3://warehouse").rstrip('/') + "/order_errors/fact_order_errors', allow_moved_paths = true)"
    )


with DAG(
    dag_id="update_order_errors_duckdb",
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["bootstrap", "duckdb", "order_errors"],
    default_args=DEFAULT_ARGS,
) as dag:
    PythonOperator(task_id="create_views", python_callable=create_views)
