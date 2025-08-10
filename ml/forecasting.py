"""Simple forecasting utilities for stockout risk analysis."""
from __future__ import annotations

from datetime import date
from typing import Iterable, List, Dict

import duckdb


def moving_average(history: Iterable[float]) -> float:
    """Compute the arithmetic mean of a sequence.

    Raises:
        ValueError: If ``history`` is empty.
    """

    values = list(history)
    if not values:
        raise ValueError("history cannot be empty")
    return sum(values) / len(values)


def write_stockout_risk(predictions: List[Dict], db_path: str) -> int:
    """Write predictions to a DuckDB table and return inserted row count."""

    con = duckdb.connect(db_path)
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
        "INSERT INTO fact_stockout_risks VALUES (?, ?, ?, ?)",
        [
            (
                p["product_id"],
                p["predicted_date"],
                p["risk_score"],
                p["confidence"],
            )
            for p in predictions
        ],
    )
    con.close()
    return len(predictions)


def validate_stockout_risk(db_path: str) -> int:
    """Validate stored predictions are within bounds and return row count."""

    con = duckdb.connect(db_path)
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

