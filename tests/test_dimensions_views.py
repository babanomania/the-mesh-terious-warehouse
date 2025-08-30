import pytest
from .duckdb_view_utils import ensure_view, validate_view, iceberg_path


@pytest.mark.parametrize(
    "schema, view, path",
    [
        ("dimensions", "dim_product", iceberg_path("dimensions", "dim_product")),
        ("dimensions", "dim_vehicle", iceberg_path("dimensions", "dim_vehicle")),
        ("dimensions", "dim_warehouse", iceberg_path("dimensions", "dim_warehouse")),
        ("dimensions", "dim_error_code", iceberg_path("dimensions", "dim_error_code")),
        ("dimensions", "dim_date", iceberg_path("dimensions", "dim_date")),
    ],
)
def test_dimension_view(duck_con, schema, view, path):
    created, reason = ensure_view(duck_con, schema, view, path)
    assert created, f"{schema}.{view} not created: {reason}"

    ok, msg = validate_view(duck_con, schema, view)
    assert ok, f"{schema}.{view} validation failed (must be non-empty): {msg}"
