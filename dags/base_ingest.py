import json
import logging
import os
from datetime import timedelta
from typing import Dict, List, Type

import pika
import pyarrow as pa
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

import datetime
from airflow.utils import timezone

def days_ago(n):
    # Airflow-aware UTC datetime
    return timezone.utcnow() - datetime.timedelta(days=n)

from pydantic import BaseModel, ValidationError
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table

from metadata.generated.schema.api.data.createTable import CreateTableRequest
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.ingestion.ometa.ometa_api import OpenMetadata
from metadata.generated.schema.entity.services.connections.metadata.openMetadataConnection import (
    OpenMetadataConnection,
    AuthProvider,
)

logger = logging.getLogger(__name__)

CATALOG_NAME = os.getenv("ICEBERG_CATALOG", "local")


def build_ingest_operator(
    dag_id: str,
    queue_name: str,
    table_fqn: str,
    event_model: Type[BaseModel],
    columns: List[Dict[str, str]],
    table_description: str,
    date_field: str
) -> DAG:
    """Generic factory for simple RabbitMQ → Iceberg ingestion DAGs."""

    logger.info("Building ingest DAG %s for queue %s", dag_id, queue_name)

    def register_with_openmetadata(rows_count: int) -> None:
        server_config = OpenMetadataConnection(
            hostPort=os.getenv("OPENMETADATA_HOSTPORT", "http://openmetadata:8585/api"),
            authProvider=AuthProvider.noAuth,
            # securityConfig=...  # e.g., OpenMetadataJWTClientConfig(...) if needed
        )
        metadata = OpenMetadata(server_config)

        request = CreateTableRequest(
            name=table_fqn.split(".")[-1],
            tableType="Regular",
            columns=columns + [{"name": "event_date", "dataType": "DATE"}],
            owner=EntityReference(
                id="00000000-0000-0000-0000-000000000000",
                type="user",
            ),
            description=table_description,
            # Optionally: fullyQualifiedName=table_fqn
            # databaseSchema=EntityReference(id=..., type="databaseSchema")
        )

        metadata.create_or_update(request)
        logger.info("Registered %s rows to OpenMetadata", rows_count)

    def consume_and_write() -> None:
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
                record = event.dict()
                ts_val = getattr(event, date_field)
                record["event_date"] = ts_val.date().isoformat()
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

        catalog = load_catalog(CATALOG_NAME)
        table: Table = catalog.load_table(table_fqn)
        table.append(pa.Table.from_pylist(rows))
        register_with_openmetadata(len(rows))

    return PythonOperator(
            task_id=f"consume_{queue_name}",
            python_callable=consume_and_write,
            sla=timedelta(minutes=15),
        )
