"""Curate dimensions.dim_date incrementally into Iceberg."""
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

    m = max_ts(con, "dimensions", "dim_date", "event_date")
    sql = with_ts_filter(
        """
        with dates as (
          select distinct cast(order_ts as date) as date_id from {orders_raw}
          union all
          select distinct cast(return_ts as date) as date_id from {returns_raw}
          union all
          select distinct cast(event_ts as date) as date_id from {dispatch_raw}
        )
        select
          date_id,
          extract(day from date_id)::int as day,
          strftime(date_id, '%W')::int as week,
          extract(month from date_id)::int as month,
          ((extract(month from date_id)::int - 1) / 3 + 1)::int as quarter,
          extract(year from date_id)::int as year,
          date_id as event_date
        from dates
        where date_id is not null
        """,
        "event_date",
        m,
    )
    at = con.execute(
        sql.format(
            orders_raw=table_expr("orders", "raw_orders"),
            returns_raw=table_expr("returns", "raw_returns"),
            dispatch_raw=table_expr("dispatch_logs", "raw_dispatch_logs"),
        )
    ).fetch_arrow_table()

    append_arrow_to_iceberg(
        "dimensions.dim_date",
        [
            {"name": "date_id", "dataType": "DATE"},
            {"name": "day", "dataType": "INT"},
            {"name": "week", "dataType": "INT"},
            {"name": "month", "dataType": "INT"},
            {"name": "quarter", "dataType": "INT"},
            {"name": "year", "dataType": "INT"},
            {"name": "event_date", "dataType": "DATE"},
        ],
        at,
    )


with DAG(
    dag_id="curate_dim_date",
    schedule="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["dimensions", "dim_date", "iceberg"],
    default_args=DEFAULT_ARGS,
) as dag:
    PythonOperator(task_id="curate_dim_date", python_callable=curate)
