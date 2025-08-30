"""Airflow DAG to update OpenMetadata for dimension tables only.

This DAG does not run dbt. It ensures the curated service/schema exists,
registers/updates table + column metadata for each dimension, and adds
lineage from raw upstream tables to the dimension tables.
"""
from __future__ import annotations

import os
import logging
import datetime
from datetime import timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.models.baseoperator import chain
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
            CreateDatabaseServiceRequest(name=svc_name, serviceType=svc_type, connection=None)
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
    sch_name: str = "dimensions",
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
    upstream_service: str,
    downstream_service: str,
    db_name: str,
    upstream_schema: str,
    downstream_schema: str,
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
    dag_id="update_dimensions_metadata",
    schedule="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["metadata", "dimensions"],
    default_args=DEFAULT_ARGS,
) as dag:
    ensure_curated_entities = PythonOperator(
        task_id="ensure_curated_service_database_schema",
        python_callable=_ensure_service_database_schema,
        op_kwargs={
            "svc_name": "iceberg",
            "svc_type_str": os.getenv("OM_CURATED_SERVICE_TYPE", "Iceberg"),
            "db_name": os.getenv("OM_DATABASE_NAME", "warehouse"),
            "sch_name": "dimensions",
        },
    )

    DIMENSIONS = {
        "dim_product": [
            {"name": "product_id", "dataType": "STRING", "description": "Product identifier"},
            {"name": "name", "dataType": "STRING", "description": "Product name"},
            {"name": "category", "dataType": "STRING", "description": "Product category"},
            {"name": "unit_cost", "dataType": "INT", "description": "Unit cost"},
            {"name": "event_date", "dataType": "DATE", "description": "Partition/date column"},
        ],
        "dim_warehouse": [
            {"name": "warehouse_id", "dataType": "STRING", "description": "Warehouse identifier"},
            {"name": "region", "dataType": "STRING", "description": "Region"},
            {"name": "manager", "dataType": "STRING", "description": "Warehouse manager"},
            {"name": "capacity", "dataType": "INT", "description": "Capacity"},
            {"name": "event_date", "dataType": "DATE", "description": "Partition/date column"},
        ],
        "dim_employee": [
            {"name": "employee_id", "dataType": "STRING", "description": "Employee identifier"},
            {"name": "role", "dataType": "STRING", "description": "Role title"},
            {"name": "assigned_warehouse", "dataType": "STRING", "description": "Warehouse assignment"},
            {"name": "shift_hours", "dataType": "INT", "description": "Shift hours"},
            {"name": "event_date", "dataType": "DATE", "description": "Partition/date column"},
        ],
        "dim_vehicle": [
            {"name": "vehicle_id", "dataType": "STRING", "description": "Vehicle identifier"},
            {"name": "type", "dataType": "STRING", "description": "Vehicle type"},
            {"name": "capacity", "dataType": "INT", "description": "Capacity"},
            {"name": "current_location", "dataType": "STRING", "description": "Current location text"},
            {"name": "event_date", "dataType": "DATE", "description": "Partition/date column"},
        ],
        "dim_route": [
            {"name": "route_id", "dataType": "STRING", "description": "Route identifier"},
            {"name": "region_covered", "dataType": "STRING", "description": "Region covered"},
            {"name": "avg_duration", "dataType": "INT", "description": "Average duration (minutes)"},
            {"name": "event_date", "dataType": "DATE", "description": "Partition/date column"},
        ],
        "dim_error_code": [
            {"name": "error_code", "dataType": "STRING", "description": "Error code"},
            {"name": "description", "dataType": "STRING", "description": "Description"},
            {"name": "severity_level", "dataType": "STRING", "description": "Severity label"},
            {"name": "event_date", "dataType": "DATE", "description": "Partition/date column"},
        ],
        "dim_date": [
            {"name": "date_id", "dataType": "DATE", "description": "Date key"},
            {"name": "day", "dataType": "INT", "description": "Day of month"},
            {"name": "week", "dataType": "INT", "description": "ISO week number"},
            {"name": "month", "dataType": "INT", "description": "Month number"},
            {"name": "quarter", "dataType": "INT", "description": "Quarter number"},
            {"name": "year", "dataType": "INT", "description": "Year"},
            {"name": "event_date", "dataType": "DATE", "description": "Partition/date column"},
        ],
    }

    update_tasks = []
    for tname, cols in DIMENSIONS.items():
        update_tasks.append(
            PythonOperator(
                task_id=f"update_{tname}_metadata",
                python_callable=_update_table_metadata,
                op_kwargs={
                    "table_name": tname,
                    "description": f"Dimension table {tname} curated to Iceberg via Airflow.",
                    "columns": cols,
                    "create_if_missing": True,
                    "svc_name": "iceberg",
                    "db_name": os.getenv("OM_DATABASE_NAME", "warehouse"),
                    "sch_name": "dimensions",
                },
            )
        )

    lineage_edges = [
        ("orders", "raw_orders", "dim_product"),
        ("orders", "raw_orders", "dim_warehouse"),
        ("dispatch_logs", "raw_dispatch_logs", "dim_vehicle"),
        ("returns", "raw_returns", "dim_error_code"),
    ]

    lineage_tasks = []
    for upstream_schema, upstream_table, dim_table in lineage_edges:
        lineage_tasks.append(
            PythonOperator(
                task_id=f"lineage_{upstream_schema}_{dim_table}",
                python_callable=_ensure_lineage,
                op_kwargs={
                    "upstream_table": upstream_table,
                    "downstream_table": dim_table,
                    "upstream_service": "rabbit_mq",
                    "downstream_service": "iceberg",
                    "db_name": os.getenv("OM_DATABASE_NAME", "warehouse"),
                    "upstream_schema": upstream_schema,
                    "downstream_schema": "dimensions",
                },
            )
        )

    for upstream_schema, upstream_table in [
        ("orders", "raw_orders"),
        ("returns", "raw_returns"),
        ("dispatch_logs", "raw_dispatch_logs"),
    ]:
        lineage_tasks.append(
            PythonOperator(
                task_id=f"lineage_{upstream_schema}_dim_date",
                python_callable=_ensure_lineage,
                op_kwargs={
                    "upstream_table": upstream_table,
                    "downstream_table": "dim_date",
                    "upstream_service": "rabbit_mq",
                    "downstream_service": "iceberg",
                    "db_name": os.getenv("OM_DATABASE_NAME", "warehouse"),
                    "upstream_schema": upstream_schema,
                    "downstream_schema": "dimensions",
                },
            )
        )

    # Chain tasks: ensure_curated -> all update tasks -> all lineage tasks
    chain(ensure_curated_entities, *update_tasks, *lineage_tasks)
