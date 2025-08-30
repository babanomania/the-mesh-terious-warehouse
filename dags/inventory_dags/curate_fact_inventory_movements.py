"""Curate inventory.fact_inventory_movements incrementally into Iceberg."""
from __future__ import annotations

import datetime
from datetime import timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils import timezone

from utils.iceberg_duckdb import duckdb_attach_iceberg, max_ts, with_ts_filter, append_arrow_to_iceberg, table_expr


def days_ago(n: int) -> datetime.datetime:
    return timezone.utcnow() - datetime.timedelta(days=n)


DEFAULT_ARGS = {"owner": "data-eng", "retries": 0, "sla": timedelta(minutes=15)}


def curate() -> None:
    import duckdb

    con = duckdb.connect(database=":memory:")
    duckdb_attach_iceberg(con)

    m = max_ts(con, "inventory", "fact_inventory_movements", "movement_ts")
    sql = """
        select
          event_id, event_ts, event_type,
          product_id,
          cast(event_ts as timestamp) as movement_ts,
          delta_qty,
          source_type as movement_type,
          event_date
        from {raw}
    """
    sql = sql.format(raw=table_expr("inventory", "raw_inventory_movements"))
    sql = with_ts_filter(sql, "movement_ts", m)
    at = con.execute(sql).fetch_arrow_table()

    append_arrow_to_iceberg(
        "inventory.fact_inventory_movements",
        [
            {"name": "event_id", "dataType": "STRING"},
            {"name": "event_ts", "dataType": "TIMESTAMP"},
            {"name": "event_type", "dataType": "STRING"},
            {"name": "product_id", "dataType": "STRING"},
            {"name": "movement_ts", "dataType": "TIMESTAMP"},
            {"name": "delta_qty", "dataType": "LONG"},
            {"name": "movement_type", "dataType": "STRING"},
            {"name": "event_date", "dataType": "DATE"},
        ],
        at,
    )


with DAG(
    dag_id="curate_fact_inventory_movements",
    schedule="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["inventory", "fact", "iceberg"],
    default_args=DEFAULT_ARGS,
) as dag:
    PythonOperator(task_id="curate_fact_inventory_movements", python_callable=curate)
