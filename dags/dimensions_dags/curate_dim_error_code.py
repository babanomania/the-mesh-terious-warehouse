"""Curate dimensions.dim_error_code incrementally into Iceberg."""
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

    m = max_ts(con, "dimensions", "dim_error_code", "event_date")
    sql = with_ts_filter(
        """
        with src as (
          select reason_code as error_code, cast(return_ts as date) as event_date
          from {raw}
          where reason_code is not null and reason_code <> ''
        )
        select error_code,
               cast(null as varchar) as description,
               cast(null as varchar) as severity_level,
               event_date
        from src
        """,
        "event_date",
        m,
    )
    at = con.execute(sql.format(raw=table_expr("returns", "raw_returns"))).fetch_arrow_table()

    append_arrow_to_iceberg(
        "dimensions.dim_error_code",
        [
            {"name": "error_code", "dataType": "STRING"},
            {"name": "description", "dataType": "STRING"},
            {"name": "severity_level", "dataType": "STRING"},
            {"name": "event_date", "dataType": "DATE"},
        ],
        at,
    )


with DAG(
    dag_id="curate_dim_error_code",
    schedule="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["dimensions", "dim_error_code", "iceberg"],
    default_args=DEFAULT_ARGS,
) as dag:
    PythonOperator(task_id="curate_dim_error_code", python_callable=curate)
