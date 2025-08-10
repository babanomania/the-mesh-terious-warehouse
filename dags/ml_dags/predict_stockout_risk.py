"""Airflow DAG to predict stockout risks."""
from __future__ import annotations

import logging
from datetime import date, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
from airflow.utils import timezone

def days_ago(n):
    return timezone.utcnow() - datetime.timedelta(days=n)

from ml.forecasting import write_stockout_risk

logger = logging.getLogger(__name__)


def predict_stockout() -> None:
    """Generate a minimal stockout risk prediction and persist it."""
    predictions = [
        {
            "product_id": 1,
            "predicted_date": date.today(),
            "risk_score": 0.0,
            "confidence": 1.0,
        }
    ]
    write_stockout_risk(predictions)
    logger.info(
        "Wrote %d stockout risk predictions", len(predictions)
    )


with DAG(
    dag_id="predict_stockout_risk",
    schedule="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["ml", "risk"],
    default_args={"owner": "data-eng", "retries": 1, "sla": timedelta(minutes=30)},
) as dag:
    logger.info("Configuring predict_stockout_risk DAG")
    PythonOperator(
        task_id="run_stockout_risk_prediction",
        python_callable=predict_stockout,
    )
