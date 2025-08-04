import importlib.util
import sys
from pathlib import Path


def _load_ingestion_module():
    ingestion_path = Path(__file__).resolve().parents[1] / "metadata" / "ingestion.py"
    spec = importlib.util.spec_from_file_location("ingestion", ingestion_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_build_ingestion_config_with_lineage():
    module = _load_ingestion_module()
    MetadataEntity = module.MetadataEntity
    build_ingestion_config = module.build_ingestion_config

    table = MetadataEntity(
        fqn="iceberg.default.fact_orders",
        entity_type="table",
        owner="data-eng",
        domain="orders",
        glossary_terms=["Orders"],
        sensitivity="low",
    )
    model = MetadataEntity(
        fqn="dbt.default.stg_orders",
        entity_type="dbt_model",
        owner="analytics",
        domain="orders",
    )
    dag = MetadataEntity(
        fqn="airflow.ingest_orders",
        entity_type="dag",
        owner="data-eng",
        domain="orders",
    )
    dashboard = MetadataEntity(
        fqn="superset.orders_dashboard",
        entity_type="dashboard",
    )

    config = build_ingestion_config(
        iceberg_tables=[table],
        dbt_models=[model],
        airflow_dags=[dag],
        dashboards=[dashboard],
        lineage_paths=[[dag.fqn, model.fqn, table.fqn, dashboard.fqn]],
    )

    assert table in config.entities
    assert table.owner == "data-eng"
    assert table.glossary_terms == ["Orders"]
    assert table.sensitivity == "low"

    edges = {(edge.source, edge.target) for edge in config.lineage}
    assert (dag.fqn, model.fqn) in edges
    assert (model.fqn, table.fqn) in edges
    assert (table.fqn, dashboard.fqn) in edges
