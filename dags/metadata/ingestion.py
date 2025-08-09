"""Helpers for constructing OpenMetadata ingestion configurations.

This module provides lightweight data classes for describing metadata
entities (e.g., tables, dbt models, Airflow DAGs) along with lineage
relationships between them.  It is intentionally simplified so the
resulting configuration objects can be used in tests without requiring
an actual OpenMetadata service.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, List, Sequence


@dataclass
class MetadataEntity:
    """Represents an entity to register with OpenMetadata.

    Parameters
    ----------
    fqn:
        Fully qualified name of the entity (e.g., ``iceberg.default.fact_orders``).
    entity_type:
        Type of the entity such as ``table``, ``dbt_model`` or ``dag``.
    owner:
        Optional owner of the entity.
    domain:
        Optional domain tag.
    glossary_terms:
        List of glossary terms associated with the entity.
    sensitivity:
        Optional sensitivity level (e.g., ``PII``).
    """

    fqn: str
    entity_type: str
    owner: str | None = None
    domain: str | None = None
    glossary_terms: List[str] = field(default_factory=list)
    sensitivity: str | None = None


@dataclass
class LineageEdge:
    """Represents a lineage edge between two entities."""

    source: str
    target: str


@dataclass
class IngestionConfig:
    """Container for metadata entities and their lineage."""

    entities: List[MetadataEntity] = field(default_factory=list)
    lineage: List[LineageEdge] = field(default_factory=list)

    def register(self, entity: MetadataEntity) -> None:
        """Add an entity to the configuration."""

        self.entities.append(entity)

    def add_lineage_path(self, path: Sequence[str]) -> None:
        """Create lineage edges for a series of FQNs.

        ``path`` is an ordered sequence of entity FQNs.  Each consecutive
        pair of nodes is converted into a :class:`LineageEdge`.
        """

        for src, tgt in zip(path, path[1:]):
            self.lineage.append(LineageEdge(source=src, target=tgt))


def build_ingestion_config(
    *,
    iceberg_tables: Iterable[MetadataEntity] | None = None,
    dbt_models: Iterable[MetadataEntity] | None = None,
    airflow_dags: Iterable[MetadataEntity] | None = None,
    dashboards: Iterable[MetadataEntity] | None = None,
    lineage_paths: Iterable[Sequence[str]] | None = None,
) -> IngestionConfig:
    """Create an :class:`IngestionConfig` from individual components.

    The function accepts collections of entities grouped by type along
    with optional lineage paths.  It returns a single configuration
    object that can later be consumed by an OpenMetadata workflow.
    """

    config = IngestionConfig()

    for collection in (iceberg_tables, dbt_models, airflow_dags, dashboards):
        if collection:
            for entity in collection:
                config.register(entity)

    if lineage_paths:
        for path in lineage_paths:
            config.add_lineage_path(path)

    return config
