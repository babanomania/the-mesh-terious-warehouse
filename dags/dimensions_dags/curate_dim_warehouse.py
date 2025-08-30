"""Curate dimensions.dim_warehouse incrementally into Iceberg."""
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

    m = max_ts(con, "dimensions", "dim_warehouse", "event_date")
    sql = with_ts_filter(
        """
        with src as (
          select warehouse_id, cast(order_ts as date) as event_date
          from {raw}
          where warehouse_id is not null and warehouse_id <> ''
        )
        select warehouse_id,
               cast(null as varchar) as region,
               cast(null as varchar) as manager,
               cast(null as int) as capacity,
               event_date
        from src
        """,
        "event_date",
        m,
    )
    at = con.execute(sql.format(raw=table_expr("orders", "raw_orders"))).fetch_arrow_table()

    append_arrow_to_iceberg(
        "dimensions.dim_warehouse",
        [
            {"name": "warehouse_id", "dataType": "STRING"},
            {"name": "region", "dataType": "STRING"},
            {"name": "manager", "dataType": "STRING"},
            {"name": "capacity", "dataType": "INT"},
            {"name": "event_date", "dataType": "DATE"},
        ],
        at,
    )


with DAG(
    dag_id="curate_dim_warehouse",
    schedule="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["dimensions", "dim_warehouse", "iceberg"],
    default_args=DEFAULT_ARGS,
) as dag:
    PythonOperator(task_id="curate_dim_warehouse", python_callable=curate)
