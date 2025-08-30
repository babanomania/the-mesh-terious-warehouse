"""Simple demand forecasting utilities."""
# sla placeholder
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Iterable, Mapping
import os

import duckdb


DEFAULT_DUCKDB_PATH = os.getenv(
    "DUCKDB_PATH", "s3://warehouse/ml_insights/raw_stockout_risk"
)


def _connect(db_path: str) -> duckdb.DuckDBPyConnection:
    if db_path.startswith("s3://"):
        con = duckdb.connect(
            ":memory:",
            config={
                "s3_endpoint": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
                "s3_access_key_id": os.getenv(
                    "MINIO_ROOT_USER", "minioadmin"
                ),
                "s3_secret_access_key": os.getenv(
                    "MINIO_ROOT_PASSWORD", "minioadmin"
                ),
                "s3_url_style": "path",
                "s3_use_ssl": "false",
            },
        )
        con.execute("INSTALL httpfs; LOAD httpfs; INSTALL iceberg; LOAD iceberg")
        return con
    return duckdb.connect(db_path)

def moving_average(history: Iterable[float]) -> float:
    """Return the arithmetic mean of historical demand values.

    Parameters
    ----------
    history:
        Iterable of numeric demand observations.

    Returns
    -------
    float
        The average value of the provided history.

    Raises
    ------
    ValueError
        If *history* is empty.
    """
    history_list = list(history)
    if not history_list:
        raise ValueError("history must contain at least one value")
    return sum(history_list) / len(history_list)

def write_stockout_risk(
    predictions: Iterable[Mapping[str, Any]],
    db_path: str = DEFAULT_DUCKDB_PATH,
) -> int:
    """Persist stockout risk predictions to a DuckDB or Iceberg table.

    Parameters
    ----------
    predictions:
        Iterable of mapping objects containing ``product_id``,
        ``predicted_date``, ``risk_score`` and ``confidence``.
    db_path:
        Path to the DuckDB database file where the table resides.

    Returns
    -------
    int
        The number of prediction rows written to the table.
    """

    con = _connect(db_path)

    rows = []
    for record in predictions:
        predicted_date = record["predicted_date"]
        if isinstance(predicted_date, (date, datetime)):
            predicted_date = predicted_date.isoformat()
        rows.append(
            (
                record["product_id"],
                predicted_date,
                record["risk_score"],
                record["confidence"],
            )
        )

    if rows:
        if db_path.startswith("s3://"):
            con.execute(
                """
                CREATE TEMPORARY TABLE tmp_stockout (
                    product_id INTEGER,
                    predicted_date DATE,
                    risk_score DOUBLE,
                    confidence DOUBLE
                )
                """
            )
            con.executemany(
                "INSERT INTO tmp_stockout VALUES (?, ?, ?, ?)", rows
            )
            con.execute(
                f"COPY tmp_stockout TO '{db_path}' (FORMAT ICEBERG)"
            )
        else:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS fact_stockout_risks (
                    product_id INTEGER,
                    predicted_date DATE,
                    risk_score DOUBLE,
                    confidence DOUBLE
                )
                """
            )
            con.executemany(
                "INSERT INTO fact_stockout_risks VALUES (?, ?, ?, ?)", rows
            )
    con.close()
    return len(rows)

def validate_stockout_risk(
    db_path: str = DEFAULT_DUCKDB_PATH,
) -> int:
    """Validate stockout risk predictions stored in DuckDB or Iceberg.

    The function checks that ``risk_score`` and ``confidence`` values in
    ``fact_stockout_risks`` fall within the inclusive range ``[0, 1]``.

    Parameters
    ----------
    db_path:
        Path to the DuckDB database file containing the
        ``fact_stockout_risks`` table.

    Returns
    -------
    int
        The number of rows validated.

    Raises
    ------
    ValueError
        If any ``risk_score`` or ``confidence`` is outside the
        ``[0, 1]`` range.
    """

    con = _connect(db_path)
    if db_path.startswith("s3://"):
        results = con.execute(
            f"SELECT risk_score, confidence FROM read_iceberg('{db_path}')"
        ).fetchall()
    else:
        results = con.execute(
            "SELECT risk_score, confidence FROM fact_stockout_risks"
        ).fetchall()
    con.close()

    for risk_score, confidence in results:
        if not 0 <= risk_score <= 1:
            raise ValueError("risk_score must be between 0 and 1")
        if not 0 <= confidence <= 1:
            raise ValueError("confidence must be between 0 and 1")

    return len(results)
