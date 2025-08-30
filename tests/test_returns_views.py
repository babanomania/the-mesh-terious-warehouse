import pytest
from .duckdb_view_utils import ensure_view, validate_view, iceberg_path


def test_returns_fact_view(duck_con):
    schema = "returns"
    view = "fact_returns"
    path = iceberg_path("returns", "fact_returns")
    created, reason = ensure_view(duck_con, schema, view, path)
    assert created, f"{schema}.{view} not created: {reason}"
    ok, msg = validate_view(duck_con, schema, view)
    assert ok, f"{schema}.{view} validation failed (must be non-empty): {msg}"
