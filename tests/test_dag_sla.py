from pathlib import Path
import pytest

DAG_DIRECTORY = Path(__file__).resolve().parents[1] / "dags"

@pytest.mark.parametrize("dag_path", list(DAG_DIRECTORY.rglob("*.py")))
def test_dag_has_sla_or_builder(dag_path):
    """Ensure each DAG configures an SLA or uses the base ingest builder."""
    source = dag_path.read_text()
    assert ("sla" in source) or ("build_ingest_dag" in source), f"{dag_path} missing SLA configuration"
