from __future__ import annotations

import os
import datetime
from datetime import timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils import timezone


def days_ago(n: int) -> datetime.datetime:
    return timezone.utcnow() - datetime.timedelta(days=n)


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


def _ensure_service_database_schema(svc_name: str, svc_type_str: str, db_name: str, sch_name: str) -> str:
    from metadata.generated.schema.entity.services.databaseService import DatabaseService, DatabaseServiceType
    from metadata.generated.schema.api.services.createDatabaseService import CreateDatabaseServiceRequest
    from metadata.generated.schema.entity.data.database import Database
    from metadata.generated.schema.api.data.createDatabase import CreateDatabaseRequest
    from metadata.generated.schema.entity.data.databaseSchema import DatabaseSchema
    from metadata.generated.schema.api.data.createDatabaseSchema import CreateDatabaseSchemaRequest

    db_fqn = f"{svc_name}.{db_name}"
    schema_fqn = f"{svc_name}.{db_name}.{sch_name}"

    try:
        svc_type = next(t for t in DatabaseServiceType if t.name.lower() == svc_type_str.lower())
    except StopIteration:
        raise RuntimeError("Unknown OM_SERVICE_TYPE")

    md = _get_metadata_client()

    service = md.get_by_name(entity=DatabaseService, fqn=svc_name)
    if not service:
        md.create_or_update(CreateDatabaseServiceRequest(name=svc_name, serviceType=svc_type, connection=None))
    database = md.get_by_name(entity=Database, fqn=db_fqn)
    if not database:
        database = md.create_or_update(CreateDatabaseRequest(name=db_name, service=svc_name))
    schema = md.get_by_name(entity=DatabaseSchema, fqn=schema_fqn)
    if not schema:
        schema = md.create_or_update(CreateDatabaseSchemaRequest(name=sch_name, database=db_fqn))
    return schema_fqn


def _update_table_metadata(table_name: str, description: str, columns: list, svc_name: str = "iceberg", db_name: str = "warehouse", sch_name: str = "ml_insights") -> None:
    from metadata.generated.schema.entity.data.table import Table
    from metadata.generated.schema.api.data.createTable import CreateTableRequest

    schema_fqn = f"{svc_name}.{db_name}.{sch_name}"
    table_fqn = f"{schema_fqn}.{table_name}"

    md = _get_metadata_client()
    entity = md.get_by_name(entity=Table, fqn=table_fqn)
    req = CreateTableRequest(name=table_name, tableType="Regular", columns=columns, description=description, databaseSchema=schema_fqn)
    md.create_or_update(req)


with DAG(
    dag_id="update_ml_insights_metadata",
    schedule="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["metadata", "ml"],
    default_args={"owner": "data-eng", "retries": 0, "sla": timedelta(minutes=15)},
) as dag:
    ensure = PythonOperator(
        task_id="ensure_service_db_schema",
        python_callable=_ensure_service_database_schema,
        op_kwargs={
            "svc_name": "iceberg",
            "svc_type_str": os.getenv("OM_CURATED_SERVICE_TYPE", "Iceberg"),
            "db_name": os.getenv("OM_DATABASE_NAME", "warehouse"),
            "sch_name": "ml_insights",
        },
    )

    stockout_cols = [
        {"name": "event_id", "dataType": "STRING"},
        {"name": "event_ts", "dataType": "TIMESTAMP"},
        {"name": "event_type", "dataType": "STRING"},
        {"name": "product_id", "dataType": "LONG"},
        {"name": "risk_score", "dataType": "LONG"},
        {"name": "predicted_date", "dataType": "DATE"},
        {"name": "confidence", "dataType": "LONG"},
        {"name": "event_date", "dataType": "DATE"},
    ]

    forecast_cols = [
        {"name": "event_id", "dataType": "STRING"},
        {"name": "event_ts", "dataType": "TIMESTAMP"},
        {"name": "event_type", "dataType": "STRING"},
        {"name": "product_id", "dataType": "STRING"},
        {"name": "region", "dataType": "STRING"},
        {"name": "forecast_ts", "dataType": "TIMESTAMP"},
        {"name": "forecast_qty", "dataType": "LONG"},
        {"name": "event_date", "dataType": "DATE"},
    ]

    update_stockout = PythonOperator(
        task_id="update_raw_stockout_risk_metadata",
        python_callable=_update_table_metadata,
        op_kwargs={
            "table_name": "raw_stockout_risk",
            "description": "Raw stockout risk predictions emitted by ML jobs.",
            "columns": stockout_cols,
            "svc_name": "iceberg",
            "db_name": os.getenv("OM_DATABASE_NAME", "warehouse"),
            "sch_name": "ml_insights",
        },
    )

    update_forecast = PythonOperator(
        task_id="update_raw_demand_forecast_metadata",
        python_callable=_update_table_metadata,
        op_kwargs={
            "table_name": "raw_demand_forecast",
            "description": "Raw demand forecasts emitted by ML jobs.",
            "columns": forecast_cols,
            "svc_name": "iceberg",
            "db_name": os.getenv("OM_DATABASE_NAME", "warehouse"),
            "sch_name": "ml_insights",
        },
    )

    ensure >> [update_stockout, update_forecast]

