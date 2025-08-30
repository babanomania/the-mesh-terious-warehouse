import pytest
from .duckdb_view_utils import connect_duckdb


@pytest.fixture(scope="session")
def duck_con():
    try:
        con = connect_duckdb()
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"DuckDB not available or misconfigured: {exc}")
    return con

