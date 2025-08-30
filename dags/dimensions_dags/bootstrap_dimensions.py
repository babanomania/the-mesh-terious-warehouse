"""Bootstrap Iceberg-backed dimension tables so dbt views can read them.

Creates empty Iceberg tables in the `dimensions` namespace using PyIceberg
with schemas aligned to dbt models. This runs idempotently.
"""
from __future__ import annotations

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import timedelta
import logging

from pathlib import Path
import datetime

from base_ingest import _load_iceberg_catalog, _ensure_table_exists, days_ago

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {"owner": "data-eng", "retries": 1, "sla": timedelta(minutes=30)}


DIM_TABLE_SPECS = {
    "dimensions.dim_product": [
        {"name": "product_id", "dataType": "STRING"},
        {"name": "name", "dataType": "STRING"},
        {"name": "category", "dataType": "STRING"},
        {"name": "unit_cost", "dataType": "INT"},
    ],
    "dimensions.dim_warehouse": [
        {"name": "warehouse_id", "dataType": "STRING"},
        {"name": "region", "dataType": "STRING"},
        {"name": "manager", "dataType": "STRING"},
        {"name": "capacity", "dataType": "INT"},
    ],
    "dimensions.dim_employee": [
        {"name": "employee_id", "dataType": "STRING"},
        {"name": "role", "dataType": "STRING"},
        {"name": "assigned_warehouse", "dataType": "STRING"},
        {"name": "shift_hours", "dataType": "INT"},
    ],
    "dimensions.dim_vehicle": [
        {"name": "vehicle_id", "dataType": "STRING"},
        {"name": "type", "dataType": "STRING"},
        {"name": "capacity", "dataType": "INT"},
        {"name": "current_location", "dataType": "STRING"},
    ],
    "dimensions.dim_route": [
        {"name": "route_id", "dataType": "STRING"},
        {"name": "region_covered", "dataType": "STRING"},
        {"name": "avg_duration", "dataType": "INT"},
    ],
    "dimensions.dim_error_code": [
        {"name": "error_code", "dataType": "STRING"},
        {"name": "description", "dataType": "STRING"},
        {"name": "severity_level", "dataType": "STRING"},
    ],
    "dimensions.dim_date": [
        {"name": "date_id", "dataType": "DATE"},
        {"name": "day", "dataType": "INT"},
        {"name": "week", "dataType": "INT"},
        {"name": "month", "dataType": "INT"},
        {"name": "quarter", "dataType": "INT"},
        {"name": "year", "dataType": "INT"},
    ],
}


def create_dimensions() -> None:
    catalog = _load_iceberg_catalog()
    for fqn, cols in DIM_TABLE_SPECS.items():
        _ensure_table_exists(catalog, fqn, cols)
        logger.info("Ensured Iceberg table exists: %s", fqn)


with DAG(
    dag_id="bootstrap_dimensions",
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["dimensions", "bootstrap"],
    default_args=DEFAULT_ARGS,
) as dag:
    PythonOperator(task_id="create_dimensions", python_callable=create_dimensions)
