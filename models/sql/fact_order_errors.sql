CREATE TABLE IF NOT EXISTS iceberg.fact_order_errors (
    event_id STRING,
    event_ts TIMESTAMP,
    event_type STRING,
    error_id STRING,
    order_id STRING,
    error_code STRING,
    detected_ts TIMESTAMP,
    event_date DATE
)
USING iceberg
PARTITIONED BY (event_date);
