import pytest
from .duckdb_view_utils import ensure_view, validate_view, iceberg_path


def test_dispatch_logs_fact_view(duck_con):
    schema = "dispatch_logs"
    view = "fact_dispatch_logs"
    path = iceberg_path("dispatch_logs", "fact_dispatch_logs")
    created, reason = ensure_view(duck_con, schema, view, path)
    assert created, f"{schema}.{view} not created: {reason}"
    ok, msg = validate_view(duck_con, schema, view)
    assert ok, f"{schema}.{view} validation failed (must be non-empty): {msg}"
