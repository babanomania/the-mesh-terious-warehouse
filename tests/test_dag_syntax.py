import ast
from pathlib import Path

import pytest

DAG_DIRECTORY = Path(__file__).resolve().parents[1] / "dags"

@pytest.mark.parametrize("dag_path", list(DAG_DIRECTORY.rglob("*.py")))
def test_dag_files_are_syntactically_valid(dag_path):
    """Ensure each Airflow DAG is syntactically correct."""
    source = dag_path.read_text()
    ast.parse(source, filename=str(dag_path))
