import importlib.util
from pathlib import Path
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
