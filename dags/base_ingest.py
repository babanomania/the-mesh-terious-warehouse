import json
import logging
import os
from datetime import timedelta
from typing import Dict, List, Type

import pika
import pyarrow as pa
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pydantic import BaseModel, ValidationError
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
from metadata.generated.schema.api.data.createTable import CreateTableRequest
from metadata.generated.schema.type.entityReference import EntityReference
from metadata.ingestion.ometa.config import OpenMetadataServerConfig
from metadata.ingestion.ometa.openmetadata import OpenMetadata

CATALOG_NAME = os.getenv("ICEBERG_CATALOG", "local")


def build_ingest_dag(
    dag_id: str,
    queue_name: str,
    table_fqn: str,
    event_model: Type[BaseModel],
    columns: List[Dict[str, str]],
    table_description: str,
    date_field: str,
    schedule: str = "@hourly",
) -> DAG:
    """Generic factory for simple RabbitMQ → Iceberg ingestion DAGs."""

    def register_with_openmetadata(rows_count: int) -> None:
        server_config = OpenMetadataServerConfig(
            hostPort=os.getenv("OPENMETADATA_HOSTPORT", "http://localhost:8585/api"),
            authProvider="no-auth",
        )
        metadata = OpenMetadata(server_config)
        request = CreateTableRequest(
            name=table_fqn.split(".")[-1],
            tableType="Regular",
            columns=columns + [{"name": "event_date", "dataType": "DATE"}],
            owner=EntityReference(id="00000000-0000-0000-0000-000000000000", type="user"),
            description=table_description,
        )
        metadata.create_or_update(request)
        logging.info("Registered %s rows to OpenMetadata", rows_count)

    def consume_and_write() -> None:
        credentials = pika.PlainCredentials(
            os.getenv("RABBITMQ_USER", "guest"),
            os.getenv("RABBITMQ_PASSWORD", "guest"),
        )
        parameters = pika.ConnectionParameters(
            host=os.getenv("RABBITMQ_HOST", "localhost"),
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
                logging.error("Validation error: %s", exc)
                channel.basic_nack(method_frame.delivery_tag, requeue=False)

        channel.close()
        connection.close()

        if not rows:
            logging.info("No messages consumed")
            return

        catalog = load_catalog(CATALOG_NAME)
        table: Table = catalog.load_table(table_fqn)
        table.append(pa.Table.from_pylist(rows))
        register_with_openmetadata(len(rows))

    with DAG(
        dag_id=dag_id,
        schedule_interval=schedule,
        start_date=days_ago(1),
        catchup=False,
        default_args={"owner": "data-eng", "retries": 1},
    ) as dag:
        PythonOperator(
            task_id=f"consume_{queue_name}",
            python_callable=consume_and_write,
            sla=timedelta(minutes=15),
        )
    return dag
