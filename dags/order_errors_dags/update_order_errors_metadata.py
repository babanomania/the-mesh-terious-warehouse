"""Airflow DAG to update OpenMetadata descriptions for order_errors datasets.

This DAG updates the descriptions for:
- order_errors.raw_order_errors (raw table)
- order_errors.fact_order_errors (fact view/table)

It only talks to OpenMetadata to upsert descriptions on existing entities
or create them if missing, then adds lineage edges between them.
"""
from __future__ import annotations

import logging
import os
import datetime
from datetime import timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils import timezone


def days_ago(n: int) -> datetime.datetime:
    return timezone.utcnow() - datetime.timedelta(days=n)


logger = logging.getLogger(__name__)


def _get_metadata_client():
    from metadata.ingestion.ometa.ometa_api import OpenMetadata
    from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
        OpenMetadataConnection,
        AuthProvider,
    )
    from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
        OpenMetadataJWTClientConfig,
    )

    server_config = OpenMetadataConnection(
        hostPort=os.getenv("OPENMETADATA_HOSTPORT", "http://openmetadata:8585/api"),
        authProvider=AuthProvider.basic,
        securityConfig=OpenMetadataJWTClientConfig(
            jwtToken=os.getenv("OPENMETADATA_JWT_TOKEN")
        ),
    )
    return OpenMetadata(server_config)


def _ensure_service_database_schema(
    svc_name: str,
    svc_type_str: str,
    db_name: str,
    sch_name: str,
) -> str:
    from metadata.generated.schema.entity.services.databaseService import (
        DatabaseService,
        DatabaseServiceType,
    )
    from metadata.generated.schema.api.services.createDatabaseService import (
        CreateDatabaseServiceRequest,
    )
    from metadata.generated.schema.entity.data.database import Database
    from metadata.generated.schema.api.data.createDatabase import CreateDatabaseRequest
    from metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema
    from metadata.generated.schema.api.data.createDatabaseSchema import (
        CreateDatabaseSchemaRequest,
    )

    # Inputs are passed in per-zone (raw vs curated)

    db_fqn = f"{svc_name}.{db_name}"
    schema_fqn = f"{svc_name}.{db_name}.{sch_name}"

    try:
        svc_type = next(t for t in DatabaseServiceType if t.name.lower() == svc_type_str.lower())
    except StopIteration:
        valid = ", ".join(t.name for t in DatabaseServiceType)
        raise RuntimeError(f"Unknown OM_SERVICE_TYPE '{svc_type_str}'. Valid: {valid}")

    md = _get_metadata_client()

    service = md.get_by_name(entity=DatabaseService, fqn=svc_name)
    if not service:
        md.create_or_update(
            CreateDatabaseServiceRequest(
                name=svc_name,
                serviceType=svc_type,
                connection=None,
            )
        )
        service = md.get_by_name(entity=DatabaseService, fqn=svc_name)
        if not service:
            raise RuntimeError(f"Failed to create or fetch DatabaseService '{svc_name}'")

    database = md.get_by_name(entity=Database, fqn=db_fqn)
    if not database:
        database = md.create_or_update(CreateDatabaseRequest(name=db_name, service=svc_name))

    schema = md.get_by_name(entity=DatabaseSchema, fqn=schema_fqn)
    if not schema:
        schema = md.create_or_update(CreateDatabaseSchemaRequest(name=sch_name, database=db_fqn))

    return schema_fqn


def _update_table_metadata(
    table_name: str,
    description: str,
    columns: list | None = None,
    create_if_missing: bool = True,
    svc_name: str = "dbt",
    db_name: str = "warehouse",
    sch_name: str = "order_errors",
) -> None:
    schema_fqn = f"{svc_name}.{db_name}.{sch_name}"
    table_fqn = f"{schema_fqn}.{table_name}"

    from metadata.generated.schema.entity.data.table import Table
    from metadata.generated.schema.api.data.createTable import CreateTableRequest

    metadata = _get_metadata_client()

    entity = metadata.get_by_name(entity=Table, fqn=table_fqn)
    if not entity:
        if not create_if_missing:
            logger.warning("OpenMetadata entity not found (skipping): %s", table_fqn)
            return
        req = CreateTableRequest(
            name=table_name,
            tableType="Regular",
            columns=columns or [],
            description=description,
            databaseSchema=schema_fqn,
        )
        metadata.create_or_update(req)
        logger.info("Created table entity and set metadata for %s", table_fqn)
        return

    name_val = getattr(entity.name, "__root__", entity.name)
    req = CreateTableRequest(
        name=name_val,
        tableType=entity.tableType,
        columns=columns if columns is not None else entity.columns,
        description=description,
        databaseSchema=schema_fqn,
    )
    metadata.create_or_update(req)
    logger.info("Updated metadata for %s", table_fqn)


def _ensure_lineage(
    upstream_table: str,
    downstream_table: str,
    upstream_service: str = "rabbit_mq",
    downstream_service: str = "duckdb",
    db_name: str = "warehouse",
    upstream_schema: str = "order_errors",
    downstream_schema: str = "order_errors",
) -> None:
    up_fqn = f"{upstream_service}.{db_name}.{upstream_schema}.{upstream_table}"
    down_fqn = f"{downstream_service}.{db_name}.{downstream_schema}.{downstream_table}"

    from metadata.generated.schema.entity.data.table import Table
    from metadata.generated.schema.type.entityReference import EntityReference
    from metadata.generated.schema.type.entityLineage import EntitiesEdge
    from metadata.generated.schema.api.lineage.addLineage import AddLineageRequest

    metadata = _get_metadata_client()

    up = metadata.get_by_name(entity=Table, fqn=up_fqn)
    down = metadata.get_by_name(entity=Table, fqn=down_fqn)
    if not up or not down:
        logger.warning("Cannot create lineage, missing entities: up=%s, down=%s", up_fqn, down_fqn)
        return

    edge = EntitiesEdge(
        fromEntity=EntityReference(id=up.id, type="table"),
        toEntity=EntityReference(id=down.id, type="table"),
    )
    metadata.add_lineage(AddLineageRequest(edge=edge))
    logger.info("Added lineage: %s -> %s", up_fqn, down_fqn)


DEFAULT_ARGS = {"owner": "data-eng", "retries": 0, "sla": timedelta(minutes=15)}

with DAG(
    dag_id="update_order_errors_metadata",
    schedule="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["metadata", "order_errors"],
    default_args=DEFAULT_ARGS,
) as dag:
    ensure_raw_entities = PythonOperator(
        task_id="ensure_raw_service_database_schema",
        python_callable=_ensure_service_database_schema,
        op_kwargs={
            "svc_name": "rabbit_mq",
            "svc_type_str": os.getenv("OM_RAW_SERVICE_TYPE", "Iceberg"),
            "db_name": os.getenv("OM_DATABASE_NAME", "warehouse"),
            "sch_name": "order_errors",
        },
    )
    ensure_curated_entities = PythonOperator(
        task_id="ensure_curated_service_database_schema",
        python_callable=_ensure_service_database_schema,
        op_kwargs={
            "svc_name": "dbt",
            "svc_type_str": os.getenv("OM_CURATED_SERVICE_TYPE", "Iceberg"),
            "db_name": os.getenv("OM_DATABASE_NAME", "warehouse"),
            "sch_name": "order_errors",
        },
    )

    RAW_COLUMNS = [
        {"name": "event_id", "dataType": "STRING", "description": "Unique UUID for the emitted event."},
        {"name": "event_ts", "dataType": "TIMESTAMP", "description": "Event timestamp in ISO-8601."},
        {"name": "event_type", "dataType": "STRING", "description": "Type discriminator for the event."},
        {"name": "error_id", "dataType": "STRING", "description": "Unique error identifier."},
        {"name": "order_id", "dataType": "STRING", "description": "Related order identifier."},
        {"name": "error_code", "dataType": "STRING", "description": "Error code describing the issue."},
        {"name": "detected_ts", "dataType": "TIMESTAMP", "description": "When the error was detected."},
        {"name": "event_date", "dataType": "DATE", "description": "Partition date derived from event_ts."},
    ]

    FACT_COLUMNS = [
        {"name": "error_id", "dataType": "STRING", "description": "Grain key for the error event."},
        {"name": "order_id", "dataType": "STRING", "description": "Associated order."},
        {"name": "error_code", "dataType": "STRING", "description": "Error code."},
        {"name": "detected_ts", "dataType": "TIMESTAMP", "description": "Detection timestamp."},
        {"name": "event_date", "dataType": "DATE", "description": "Partition date for analytics."},
    ]

    update_raw = PythonOperator(
        task_id="update_raw_order_errors_metadata",
        python_callable=_update_table_metadata,
        op_kwargs={
            "table_name": "raw_order_errors",
            "description": "Raw order error events ingested from RabbitMQ.",
            "columns": RAW_COLUMNS,
            "create_if_missing": True,
            "svc_name": "rabbit_mq",
            "db_name": os.getenv("OM_DATABASE_NAME", "warehouse"),
            "sch_name": "order_errors",
        },
    )

    update_fact = PythonOperator(
        task_id="update_fact_order_errors_metadata",
        python_callable=_update_table_metadata,
        op_kwargs={
            "table_name": "fact_order_errors",
            "description": "The fact_order_errors model curates error events into an analytics-ready fact table partitioned by event_date.",
            "columns": FACT_COLUMNS,
            "create_if_missing": True,
            "svc_name": "dbt",
            "db_name": os.getenv("OM_DATABASE_NAME", "warehouse"),
            "sch_name": "order_errors",
        },
    )

    lineage_raw_to_fact = PythonOperator(
        task_id="add_lineage_raw_to_fact",
        python_callable=_ensure_lineage,
        op_kwargs={
            "upstream_table": "raw_order_errors",
            "downstream_table": "fact_order_errors",
            "upstream_service": "rabbit_mq",
            "downstream_service": "duckdb",
            "db_name": os.getenv("OM_DATABASE_NAME", "warehouse"),
            "upstream_schema": "order_errors",
            "downstream_schema": "order_errors",
        },
    )

    [ensure_raw_entities, ensure_curated_entities] >> update_raw >> update_fact >> lineage_raw_to_fact
