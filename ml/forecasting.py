"""Simple forecasting utilities for stockout risk analysis."""
from __future__ import annotations

from datetime import date
from typing import Iterable, List, Dict
import os

import duckdb


DEFAULT_DUCKDB_PATH = os.getenv(
    "DUCKDB_PATH", "s3://warehouse/fact_stockout_risks"
)


def _connect(db_path: str) -> duckdb.DuckDBPyConnection:
    """Return a DuckDB connection using MinIO settings when required."""
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
    """Compute the arithmetic mean of a sequence.

    Raises:
        ValueError: If ``history`` is empty.
    """

    values = list(history)
    if not values:
        raise ValueError("history cannot be empty")
    return sum(values) / len(values)


def write_stockout_risk(
    predictions: List[Dict], db_path: str = DEFAULT_DUCKDB_PATH
) -> int:
    """Write predictions to a DuckDB or Iceberg table and return inserted row count."""

    con = _connect(db_path)
    rows = [
        (
            p["product_id"],
            p["predicted_date"],
            p["risk_score"],
            p["confidence"],
        )
        for p in predictions
    ]

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


def validate_stockout_risk(db_path: str = DEFAULT_DUCKDB_PATH) -> int:
    """Validate stored predictions (DuckDB or Iceberg) are within bounds and return row count."""

    con = _connect(db_path)
    if db_path.startswith("s3://"):
        rows = con.execute(
            f"SELECT product_id, predicted_date, risk_score, confidence FROM read_iceberg('{db_path}')"
        ).fetchall()
    else:
        rows = con.execute(
            "SELECT product_id, predicted_date, risk_score, confidence FROM fact_stockout_risks"
        ).fetchall()
    con.close()
    for _, _, risk_score, confidence in rows:
        if not (0 <= risk_score <= 1) or not (0 <= confidence <= 1):
            raise ValueError("Invalid risk_score or confidence")
    return len(rows)


__all__ = [
    "moving_average",
    "write_stockout_risk",
    "validate_stockout_risk",
]

