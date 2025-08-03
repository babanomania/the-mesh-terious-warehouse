from pathlib import Path

import pytest

SQL_DIR = Path(__file__).resolve().parents[1] / "models" / "sql"


@pytest.mark.parametrize("sql_file", list(SQL_DIR.glob("*.sql")))
def test_iceberg_ddl_compliance(sql_file: Path) -> None:
    """Ensure Iceberg tables are declared correctly.

    All DDL files must specify the Iceberg engine and fact tables must be
    partitioned by ``event_date``.
    """
    ddl = sql_file.read_text().lower()
    assert "using iceberg" in ddl, "DDL must specify USING iceberg"

    if sql_file.name.startswith("fact_"):
        assert "event_date" in ddl, "Fact tables require event_date column"
        assert (
            "partitioned by (event_date)" in ddl
        ), "Fact tables must be partitioned by event_date"
