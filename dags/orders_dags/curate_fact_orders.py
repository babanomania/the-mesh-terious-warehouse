"""Curate orders.fact_orders incrementally into Iceberg on MinIO."""
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

    m = max_ts(con, "orders", "fact_orders", "order_ts")
    sql = """
        select
          event_id, event_ts, event_type,
          order_id, product_id, warehouse_id,
          order_ts, qty, event_date
        from {raw}
    """
    sql = sql.format(raw=table_expr("orders", "raw_orders"))
    sql = with_ts_filter(sql, "order_ts", m)
    at = con.execute(sql).fetch_arrow_table()

    append_arrow_to_iceberg(
        "orders.fact_orders",
        [
            {"name": "event_id", "dataType": "STRING"},
            {"name": "event_ts", "dataType": "TIMESTAMP"},
            {"name": "event_type", "dataType": "STRING"},
            {"name": "order_id", "dataType": "STRING"},
            {"name": "product_id", "dataType": "STRING"},
            {"name": "warehouse_id", "dataType": "STRING"},
            {"name": "order_ts", "dataType": "TIMESTAMP"},
            {"name": "qty", "dataType": "LONG"},
            {"name": "event_date", "dataType": "DATE"},
        ],
        at,
    )


with DAG(
    dag_id="curate_fact_orders",
    schedule="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["orders", "fact", "iceberg"],
    default_args=DEFAULT_ARGS,
) as dag:
    PythonOperator(task_id="curate_fact_orders", python_callable=curate)
