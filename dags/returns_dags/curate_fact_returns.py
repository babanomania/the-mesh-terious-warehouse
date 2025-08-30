"""Curate returns.fact_returns incrementally into Iceberg on MinIO."""
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

    m = max_ts(con, "returns", "fact_returns", "return_ts")
    sql = """
        select
          event_id, event_ts, event_type,
          return_id, order_id,
          return_ts, reason_code, event_date
        from {raw}
    """
    sql = sql.format(raw=table_expr("returns", "raw_returns"))
    sql = with_ts_filter(sql, "return_ts", m)
    at = con.execute(sql).fetch_arrow_table()

    append_arrow_to_iceberg(
        "returns.fact_returns",
        [
            {"name": "event_id", "dataType": "STRING"},
            {"name": "event_ts", "dataType": "TIMESTAMP"},
            {"name": "event_type", "dataType": "STRING"},
            {"name": "return_id", "dataType": "STRING"},
            {"name": "order_id", "dataType": "STRING"},
            {"name": "return_ts", "dataType": "TIMESTAMP"},
            {"name": "reason_code", "dataType": "STRING"},
            {"name": "event_date", "dataType": "DATE"},
        ],
        at,
    )


with DAG(
    dag_id="curate_fact_returns",
    schedule="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["returns", "fact", "iceberg"],
    default_args=DEFAULT_ARGS,
) as dag:
    PythonOperator(task_id="curate_fact_returns", python_callable=curate)
