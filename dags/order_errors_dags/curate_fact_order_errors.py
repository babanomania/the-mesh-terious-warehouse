"""Curate order_errors.fact_order_errors incrementally into Iceberg."""
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

    m = max_ts(con, "order_errors", "fact_order_errors", "error_ts")
    sql = """
        select
          event_id, event_ts, event_type,
          error_id, order_id, error_code,
          error_ts, details, event_date
        from {raw}
    """
    sql = sql.format(raw=table_expr("order_errors", "raw_order_errors"))
    sql = with_ts_filter(sql, "error_ts", m)
    try:
        at = con.execute(sql).fetch_arrow_table()
    except Exception:
        at = None

    append_arrow_to_iceberg(
        "order_errors.fact_order_errors",
        [
            {"name": "event_id", "dataType": "STRING"},
            {"name": "event_ts", "dataType": "TIMESTAMP"},
            {"name": "event_type", "dataType": "STRING"},
            {"name": "error_id", "dataType": "STRING"},
            {"name": "order_id", "dataType": "STRING"},
            {"name": "error_code", "dataType": "STRING"},
            {"name": "error_ts", "dataType": "TIMESTAMP"},
            {"name": "details", "dataType": "STRING"},
            {"name": "event_date", "dataType": "DATE"},
        ],
        at,
    )


with DAG(
    dag_id="curate_fact_order_errors",
    schedule="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["order_errors", "fact", "iceberg"],
    default_args=DEFAULT_ARGS,
) as dag:
    PythonOperator(task_id="curate_fact_order_errors", python_callable=curate)
