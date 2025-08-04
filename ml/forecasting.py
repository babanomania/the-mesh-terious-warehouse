"""Simple demand forecasting utilities."""
from __future__ import annotations

from typing import Iterable


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
