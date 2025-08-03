import importlib.util
import json
from pathlib import Path


def _load_module(relative_path: str):
    """Dynamically load a module from a relative file path."""
    path = Path(__file__).resolve().parents[1] / relative_path
    spec = importlib.util.spec_from_file_location(path.stem, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


orders = _load_module("ingestion/producers/orders/produce_orders_north.py")
returns = _load_module("ingestion/producers/returns/produce_returns_north.py")


def _check_base_fields(event):
    for field in ("event_id", "event_ts", "event_type"):
        assert getattr(event, field), f"{field} missing"


def test_orders_event_schema():
    event = orders.generate_event()
    _check_base_fields(event)
    assert event.order_id
    assert event.product_id
    assert event.warehouse_id
    assert event.order_ts
    assert isinstance(event.qty, int)
    parsed = json.loads(event.json())
    assert parsed["event_type"] == "order_created"


def test_returns_event_schema():
    event = returns.generate_event()
    _check_base_fields(event)
    assert event.return_id
    assert event.order_id
    assert event.return_ts
    assert event.reason_code in returns.REASON_CODES
    parsed = json.loads(event.json())
    assert parsed["event_type"] == "return_created"
