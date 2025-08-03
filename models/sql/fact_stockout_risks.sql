CREATE TABLE IF NOT EXISTS iceberg.fact_stockout_risks (
    event_id STRING,
    event_ts TIMESTAMP,
    event_type STRING,
    product_id STRING,
    risk_score DOUBLE,
    predicted_date DATE,
    confidence DOUBLE,
    event_date DATE
)
USING iceberg
PARTITIONED BY (event_date);
