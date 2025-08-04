import importlib.util
from pathlib import Path
from datetime import date

import duckdb
import pytest

spec_path = Path(__file__).resolve().parents[1] / "ml/forecasting.py"
spec = importlib.util.spec_from_file_location("forecasting", spec_path)
forecasting = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(forecasting)


def test_moving_average_basic():
    history = [1, 2, 3, 4]
    assert forecasting.moving_average(history) == 2.5


def test_moving_average_empty_history():
    with pytest.raises(ValueError):
        forecasting.moving_average([])


def test_write_stockout_risk(tmp_path):
    db_path = tmp_path / "test.duckdb"
    predictions = [
        {
            "product_id": 1,
            "predicted_date": date(2024, 1, 1),
            "risk_score": 0.9,
            "confidence": 0.8,
        },
        {
            "product_id": 2,
            "predicted_date": date(2024, 1, 2),
            "risk_score": 0.1,
            "confidence": 0.5,
        },
    ]
    inserted = forecasting.write_stockout_risk(
        predictions, db_path=str(db_path)
    )
    assert inserted == 2
    con = duckdb.connect(str(db_path))
    count = con.sql("SELECT count(*) FROM fact_stockout_risks").fetchone()[0]
    assert count == 2
