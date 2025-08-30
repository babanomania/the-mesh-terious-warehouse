"""Curate dispatch_logs.fact_dispatch_logs incrementally into Iceberg."""
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

    m = max_ts(con, "dispatch_logs", "fact_dispatch_logs", "event_ts")
    sql = """
        select
          event_id, event_ts, event_type,
          dispatch_id, vehicle_id, status,
          eta as eta_ts,
          event_date
        from {raw}
    """
    sql = sql.format(raw=table_expr("dispatch_logs", "raw_dispatch_logs"))
    sql = with_ts_filter(sql, "event_ts", m)
    at = con.execute(sql).fetch_arrow_table()

    append_arrow_to_iceberg(
        "dispatch_logs.fact_dispatch_logs",
        [
            {"name": "event_id", "dataType": "STRING"},
            {"name": "event_ts", "dataType": "TIMESTAMP"},
            {"name": "event_type", "dataType": "STRING"},
            {"name": "dispatch_id", "dataType": "STRING"},
            {"name": "vehicle_id", "dataType": "STRING"},
            {"name": "status", "dataType": "STRING"},
            {"name": "eta_ts", "dataType": "TIMESTAMP"},
            {"name": "event_date", "dataType": "DATE"},
        ],
        at,
    )


with DAG(
    dag_id="curate_fact_dispatch_logs",
    schedule="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["dispatch_logs", "fact", "iceberg"],
    default_args=DEFAULT_ARGS,
) as dag:
    PythonOperator(task_id="curate_fact_dispatch_logs", python_callable=curate)
