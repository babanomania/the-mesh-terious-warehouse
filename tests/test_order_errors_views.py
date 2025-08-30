import pytest
from .duckdb_view_utils import ensure_view, validate_view, iceberg_path


def test_order_errors_fact_view(duck_con):
    schema = "order_errors"
    view = "fact_order_errors"
    path = iceberg_path("order_errors", "fact_order_errors")
    created, reason = ensure_view(duck_con, schema, view, path)
    assert created, f"{schema}.{view} not created: {reason}"
    ok, msg = validate_view(duck_con, schema, view)
    assert ok, f"{schema}.{view} validation failed (must be non-empty): {msg}"
