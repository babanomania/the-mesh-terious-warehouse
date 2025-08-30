"""Airflow DAG to update OpenMetadata descriptions for orders datasets.

This DAG updates the descriptions for:
- orders.raw_orders (raw table)
- orders.stg_orders (staging view/table)
- orders.fact_orders (fact view/table)

It intentionally does not run dbt or any model builds. It only
talks to OpenMetadata to upsert descriptions on existing entities.
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
    """Build and return an OpenMetadata client (lazy imports)."""
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


def _ensure_service_database_schema() -> str:
    """Ensure DatabaseService, Database, and DatabaseSchema exist in OpenMetadata.

    Returns the schema FQN string (service.database.schema) for convenience.
    """
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

    svc_name = os.getenv("OM_SERVICE_NAME", "airflow")
    svc_type_str = os.getenv("OM_SERVICE_TYPE", "Iceberg")
    db_name = os.getenv("OM_DATABASE_NAME", "warehouse")
    sch_name = os.getenv("OM_SCHEMA_NAME", "orders")

    db_fqn = f"{svc_name}.{db_name}"
    schema_fqn = f"{svc_name}.{db_name}.{sch_name}"

    # Normalize service type to enum
    try:
        svc_type = next(t for t in DatabaseServiceType if t.name.lower() == svc_type_str.lower())
    except StopIteration:
        valid = ", ".join(t.name for t in DatabaseServiceType)
        raise RuntimeError(f"Unknown OM_SERVICE_TYPE '{svc_type_str}'. Valid: {valid}")

    md = _get_metadata_client()

    # Ensure service
    service = md.get_by_name(entity=DatabaseService, fqn=svc_name)
    if not service:
        md.create_or_update(
            CreateDatabaseServiceRequest(
                name=svc_name,
                serviceType=svc_type,
                connection=None,  # demo/local; plug real connection config if needed
            )
        )
        service = md.get_by_name(entity=DatabaseService, fqn=svc_name)
        if not service:
            raise RuntimeError(f"Failed to create or fetch DatabaseService '{svc_name}'")

    # Ensure database
    database = md.get_by_name(entity=Database, fqn=db_fqn)
    if not database:
        database = md.create_or_update(CreateDatabaseRequest(name=db_name, service=svc_name))

    # Ensure schema
    schema = md.get_by_name(entity=DatabaseSchema, fqn=schema_fqn)
    if not schema:
        schema = md.create_or_update(CreateDatabaseSchemaRequest(name=sch_name, database=db_fqn))

    return schema_fqn


def _update_table_metadata(
    table_name: str,
    description: str,
    columns: list | None = None,
    create_if_missing: bool = True,
) -> None:
    """Update description and (optionally) columns for a table/view.

    - Only updates existing entities; skips creation to keep concerns separated.
    - If `columns` is provided, we overwrite the column list with the given
      schema (useful for adding column descriptions). Otherwise, we preserve
      the current columns from OpenMetadata.
    """
    # Names used to build FQNs inside OpenMetadata
    svc_name = os.getenv("OM_SERVICE_NAME", "airflow")
    db_name = os.getenv("OM_DATABASE_NAME", "warehouse")
    sch_name = "orders"

    schema_fqn = f"{svc_name}.{db_name}.{sch_name}"
    table_fqn = f"{schema_fqn}.{table_name}"

    from metadata.generated.schema.entity.data.table import Table
    from metadata.generated.schema.api.data.createTable import CreateTableRequest

    metadata = _get_metadata_client()

    # Fetch existing table/view
    entity = metadata.get_by_name(entity=Table, fqn=table_fqn)
    if not entity:
        if not create_if_missing:
            logger.warning("OpenMetadata entity not found (skipping): %s", table_fqn)
            return
        # Create with provided schema; require columns to create sensibly
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

    # Re-upsert with updated description and columns
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


def _ensure_lineage(upstream_table: str, downstream_table: str) -> None:
    """Add lineage edge upstream -> downstream if both entities exist."""
    svc_name = os.getenv("OM_SERVICE_NAME", "airflow")
    db_name = os.getenv("OM_DATABASE_NAME", "warehouse")
    sch_name = "orders"

    schema_fqn = f"{svc_name}.{db_name}.{sch_name}"
    up_fqn = f"{schema_fqn}.{upstream_table}"
    down_fqn = f"{schema_fqn}.{downstream_table}"

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
    dag_id="update_orders_metadata",
    schedule="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["metadata", "orders"],
    default_args=DEFAULT_ARGS,
) as dag:
    ensure_entities = PythonOperator(
        task_id="ensure_service_database_schema",
        python_callable=_ensure_service_database_schema,
    )
    # Column definitions used for documentation; adjust to match your models
    RAW_ORDERS_COLUMNS = [
        {"name": "event_id", "dataType": "STRING", "description": "Unique UUID for the emitted event."},
        {"name": "event_ts", "dataType": "TIMESTAMP", "description": "Event timestamp in ISO-8601."},
        {"name": "event_type", "dataType": "STRING", "description": "Type discriminator for the event."},
        {"name": "order_id", "dataType": "STRING", "description": "Business key for the order."},
        {"name": "product_id", "dataType": "STRING", "description": "Product identifier."},
        {"name": "warehouse_id", "dataType": "STRING", "description": "Warehouse where the order is handled."},
        {"name": "qty", "dataType": "INT", "description": "Ordered quantity."},
        {"name": "order_ts", "dataType": "TIMESTAMP", "description": "Order creation timestamp."},
        {"name": "region", "dataType": "STRING", "description": "Origin region label from the producer."},
        {"name": "event_date", "dataType": "DATE", "description": "Partition date derived from event_ts."},
    ]

    STG_ORDERS_COLUMNS = [
        {"name": "order_id", "dataType": "STRING", "description": "Order identifier (normalized)."},
        {"name": "product_id", "dataType": "STRING", "description": "Product identifier (normalized)."},
        {"name": "warehouse_id", "dataType": "STRING", "description": "Warehouse identifier (normalized)."},
        {"name": "qty", "dataType": "INT", "description": "Ordered quantity (cleaned)."},
        {"name": "order_ts", "dataType": "TIMESTAMP", "description": "Order timestamp (cleaned)."},
        {"name": "event_date", "dataType": "DATE", "description": "Partition date for downstream models."},
    ]

    FACT_ORDERS_COLUMNS = [
        {"name": "order_id", "dataType": "STRING", "description": "Order key for the fact grain."},
        {"name": "product_id", "dataType": "STRING", "description": "Product at order level."},
        {"name": "warehouse_id", "dataType": "STRING", "description": "Warehouse fulfilling the order."},
        {"name": "qty", "dataType": "INT", "description": "Quantity at order event."},
        {"name": "order_ts", "dataType": "TIMESTAMP", "description": "Event timestamp for the order."},
        {"name": "event_date", "dataType": "DATE", "description": "Partition date for analytics."},
    ]

    update_raw = PythonOperator(
        task_id="update_raw_orders_metadata",
        python_callable=_update_table_metadata,
        op_kwargs={
            "table_name": "raw_orders",
            "description": "Raw orders ingested from RabbitMQ.",
            "columns": RAW_ORDERS_COLUMNS,
            "create_if_missing": True,
        },
    )

    update_stg = PythonOperator(
        task_id="update_stg_orders_metadata",
        python_callable=_update_table_metadata,
        op_kwargs={
            "table_name": "stg_orders",
            "description": "The stg_orders model normalizes raw order events and computes the event_date partition field used by downstream models.",
            "columns": STG_ORDERS_COLUMNS,
            "create_if_missing": True,
        },
    )

    update_fact = PythonOperator(
        task_id="update_fact_orders_metadata",
        python_callable=_update_table_metadata,
        op_kwargs={
            "table_name": "fact_orders",
            "description": "The fact_orders model curates order events into an analytics-ready fact table partitioned by event_date.",
            "columns": FACT_ORDERS_COLUMNS,
            "create_if_missing": True,
        },
    )

    lineage_raw_to_stg = PythonOperator(
        task_id="add_lineage_raw_to_stg",
        python_callable=_ensure_lineage,
        op_kwargs={"upstream_table": "raw_orders", "downstream_table": "stg_orders"},
    )

    lineage_stg_to_fact = PythonOperator(
        task_id="add_lineage_stg_to_fact",
        python_callable=_ensure_lineage,
        op_kwargs={"upstream_table": "stg_orders", "downstream_table": "fact_orders"},
    )

    # Update entities first, then add lineage
    ensure_entities >> update_raw >> update_stg >> update_fact >> lineage_raw_to_stg >> lineage_stg_to_fact
