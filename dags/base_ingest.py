from __future__ import annotations

import json
import logging
import os
import datetime
from datetime import timedelta
from typing import Dict, List, Type, Optional, TYPE_CHECKING

# Only light-weight imports at module level. Heavy ones (OpenMetadata, PyIceberg,
# PyArrow, Pika) are imported lazily inside functions to avoid DagBag timeouts.
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils import timezone
from pydantic import BaseModel, ValidationError

if TYPE_CHECKING:
    # These are for type-checkers only; they won't execute at runtime.
    from pyiceberg.catalog import Catalog
    from pyiceberg.table import Table
    from pyiceberg.schema import Schema

logger = logging.getLogger(__name__)

def days_ago(n: int) -> datetime.datetime:
    """Airflow-aware UTC datetime like the legacy helper."""
    return timezone.utcnow() - datetime.timedelta(days=n)

# Default to a MinIO-backed Iceberg catalog but allow overrides via environment.
CATALOG_NAME = os.getenv("ICEBERG_CATALOG", "minio")


def _load_iceberg_catalog() -> "Catalog":
    """
    Load an Iceberg catalog configured for local or MinIO storage.

    Uses lazy imports to keep DagBag import time fast.
    """
    from pyiceberg.catalog import load_catalog  # lazy import

    if CATALOG_NAME == "minio":
        return load_catalog(
            CATALOG_NAME,
            type=os.getenv("ICEBERG_CATALOG_TYPE", "rest"),
            uri=os.getenv("ICEBERG_REST_URI", "http://iceberg-rest:8181"),
            warehouse=os.getenv("ICEBERG_WAREHOUSE", "s3://warehouse"),
            
            **{
                "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
                "s3.endpoint": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
                "s3.access-key-id": os.getenv("MINIO_ROOT_USER", "minioadmin"),
                "s3.secret-access-key": os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"),
                "s3.path-style-access": "true",
                "s3.region": os.getenv("AWS_REGION", "us-east-1"),
                "s3.signing-region": os.getenv("AWS_REGION", "us-east-1"),
            },
        )

    return load_catalog(CATALOG_NAME)

def _build_iceberg_schema(columns: List[Dict[str, str]]) -> "Schema":
    # Lazy imports so DagBag stays fast
    from pyiceberg.schema import Schema
    from pyiceberg.types import StringType, IntegerType, TimestampType, DateType, LongType
    from pyiceberg.types import NestedField

    type_map = {
        "STRING": StringType(),
        "INT": IntegerType(),
        "LONG": LongType(),
        "TIMESTAMP": TimestampType(),
        "DATE": DateType(),
    }

    # Ensure event_date is present in the schema
    cols = columns + ([{"name": "event_date", "dataType": "DATE"}]
                      if not any(c["name"] == "event_date" for c in columns)
                      else [])
    fields = []
    field_id = 1
    for c in cols:
        dt = c["dataType"].upper()
        iceberg_t = type_map.get(dt)
        if iceberg_t is None:
            raise ValueError(f"Unsupported dataType for Iceberg: {dt}")
        fields.append(NestedField(field_id, c["name"], iceberg_t, required=False))
        field_id += 1
    return Schema(*fields)

def _ensure_table_exists(catalog: "Catalog", table_fqn: str, columns: List[Dict[str, str]]) -> "Table":
    from pyiceberg.exceptions import NoSuchTableError, NamespaceAlreadyExistsError, NoSuchNamespaceError
    # Create namespace if needed (e.g., "warehouse" in "warehouse.fact_orders")
    if "." in table_fqn:
        namespace = table_fqn.rsplit(".", 1)[0]
        try:
            catalog.create_namespace(namespace)
        except NamespaceAlreadyExistsError:
            pass
        except NoSuchNamespaceError:
            # some catalog backends raise NoSuchNamespace on list/exists, ignore
            pass

    try:
        return catalog.load_table(table_fqn)
    except NoSuchTableError:
        schema = _build_iceberg_schema(columns)
        # Unpartitioned create for simplicity/robustness; you can add a partition spec later.
        return catalog.create_table(identifier=table_fqn, schema=schema)

def build_ingest_operator(
    dag_id: str,
    queue_name: str,
    table_fqn: str,
    event_model: Type[BaseModel],
    columns: List[Dict[str, str]],
    table_description: str,
    date_field: str
) -> PythonOperator:
    """Generic factory for simple RabbitMQ → Iceberg ingestion DAGs."""

    logger.info("Building ingest DAG %s for queue %s", dag_id, queue_name)

    def register_with_openmetadata(rows_count: int) -> None:
        # Lazy imports
        from metadata.ingestion.ometa.ometa_api import OpenMetadata

        # Entities & Requests
        from metadata.generated.schema.entity.services.databaseService import DatabaseService, DatabaseServiceType
        from metadata.generated.schema.api.services.createDatabaseService import CreateDatabaseServiceRequest

        from metadata.generated.schema.entity.data.database import Database
        from metadata.generated.schema.api.data.createDatabase import CreateDatabaseRequest

        from metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema
        from metadata.generated.schema.api.data.createDatabaseSchema import CreateDatabaseSchemaRequest

        from metadata.generated.schema.entity.data.table import Table
        from metadata.generated.schema.api.data.createTable import CreateTableRequest

        # Connection/auth
        from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
            OpenMetadataConnection, AuthProvider
        )
        from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
            OpenMetadataJWTClientConfig
        )

        # ------------------ Config ------------------
        svc_name = os.getenv("OM_SERVICE_NAME", "airflow")
        svc_type_str = os.getenv("OM_SERVICE_TYPE", "Iceberg")  # Iceberg/Mysql/Postgres/Snowflake/...
        db_name  = os.getenv("OM_DATABASE_NAME", "warehouse")
        sch_name = os.getenv("OM_SCHEMA_NAME", "public")

        db_fqn     = f"{svc_name}.{db_name}"
        schema_fqn = f"{svc_name}.{db_name}.{sch_name}"
        table_name = table_fqn.split(".")[-1]
        table_fqn_full = f"{schema_fqn}.{table_name}"

        # Normalize service type enum (case-insensitive)
        try:
            svc_type = next(t for t in DatabaseServiceType if t.name.lower() == svc_type_str.lower())
        except StopIteration:
            valid = ", ".join(t.name for t in DatabaseServiceType)
            raise RuntimeError(f"Unknown OM_SERVICE_TYPE '{svc_type_str}'. Valid: {valid}")

        # ------------------ Client ------------------
        server_config = OpenMetadataConnection(
            hostPort=os.getenv("OPENMETADATA_HOSTPORT", "http://openmetadata:8585/api"),
            authProvider=AuthProvider.basic,  # keep aligned with your server setup
            securityConfig=OpenMetadataJWTClientConfig(
                jwtToken=os.getenv("OPENMETADATA_JWT_TOKEN")
            ),
        )
        metadata = OpenMetadata(server_config)

        # ------------------ Ensure Service (get or create) ------------------
        service = metadata.get_by_name(entity=DatabaseService, fqn=svc_name)
        if not service:
            # For demo use: connection=None. Replace with real connection config later.
            metadata.create_or_update(
                CreateDatabaseServiceRequest(
                    name=svc_name,
                    serviceType=svc_type,
                    connection=None,
                )
            )
            service = metadata.get_by_name(entity=DatabaseService, fqn=svc_name)
            if not service:
                raise RuntimeError(f"Failed to create or fetch DatabaseService '{svc_name}'")

        # ------------------ Ensure Database (check before create) ------------------
        database = metadata.get_by_name(entity=Database, fqn=db_fqn)
        if not database:
            database = metadata.create_or_update(
                CreateDatabaseRequest(name=db_name, service=svc_name)  # service FQN is its name
            )

        # ------------------ Ensure Schema (check before create) ------------------
        schema = metadata.get_by_name(entity=DatabaseSchema, fqn=schema_fqn)
        if not schema:
            schema = metadata.create_or_update(
                CreateDatabaseSchemaRequest(name=sch_name, database=db_fqn)  # database FQN string
            )

        # ------------------ Ensure Table (check before create) ------------------
        existing_table = metadata.get_by_name(entity=Table, fqn=table_fqn_full)
        if existing_table:
            logger.info("Table already exists in OpenMetadata: %s (skipping create)", table_fqn_full)
        else:
            request = CreateTableRequest(
                name=table_name,
                tableType="Regular",
                columns=columns + [{"name": "event_date", "dataType": "DATE"}],
                description=table_description,
                databaseSchema=schema_fqn,  # 1.9.x expects STRING FQN here
            )
            metadata.create_or_update(request)
            logger.info("Created table in OpenMetadata: %s", table_fqn_full)

        logger.info(
            "OpenMetadata upsert complete: service=%s, database=%s, schema=%s, table=%s (rows=%s)",
            svc_name, db_name, sch_name, table_name, rows_count
        )


    def consume_and_write() -> None:
        # Lazy imports for runtime only
        import pika
        import pyarrow as pa
        from pyiceberg.table import Table  # type: ignore[unused-ignore]

        credentials = pika.PlainCredentials(
            os.getenv("RABBITMQ_USER", "guest"),
            os.getenv("RABBITMQ_PASSWORD", "guest"),
        )
        parameters = pika.ConnectionParameters(
            host=os.getenv("RABBITMQ_HOST", "rabbitmq"),
            port=int(os.getenv("RABBITMQ_PORT", "5672")),
            credentials=credentials,
        )
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(queue=queue_name, durable=True)

        rows = []
        for method_frame, properties, body in channel.consume(queue_name, inactivity_timeout=1):
            if body is None:
                break
            try:
                payload = json.loads(body)
                event = event_model(**payload)
                record = event.model_dump() if hasattr(event, "model_dump") else event.dict()
                ts_val = getattr(event, date_field)
                record["event_date"] = ts_val.date()
                rows.append(record)
                channel.basic_ack(method_frame.delivery_tag)
            except ValidationError as exc:
                logger.error("Validation error: %s", exc)
                channel.basic_nack(method_frame.delivery_tag, requeue=False)

        channel.close()
        connection.close()

        if not rows:
            logger.info("No messages consumed")
            return

        catalog = _load_iceberg_catalog()
        table: "Table" = _ensure_table_exists(catalog, table_fqn, columns)
        table.append(pa.Table.from_pylist(rows))
        register_with_openmetadata(len(rows))

    return PythonOperator(
        task_id=f"consume_{queue_name}",
        python_callable=consume_and_write,
        # sla=timedelta(minutes=15),
    )
