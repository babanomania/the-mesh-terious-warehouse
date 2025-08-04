"""Simple demand forecasting utilities."""
from __future__ import annotations

from datetime import date, datetime
from typing import Any, Iterable, Mapping

import duckdb


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
    db_path: str = "warehouse.duckdb",
) -> int:
    """Persist stockout risk predictions to a DuckDB table.

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
        con.executemany(
            "INSERT INTO fact_stockout_risks VALUES (?, ?, ?, ?)", rows
        )
    con.close()
    return len(rows)


def validate_stockout_risk(db_path: str = "warehouse.duckdb") -> int:
    """Validate stockout risk predictions stored in DuckDB.

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

    con = duckdb.connect(db_path)
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
