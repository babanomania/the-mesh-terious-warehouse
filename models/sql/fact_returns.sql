CREATE TABLE IF NOT EXISTS iceberg.fact_returns (
    event_id STRING,
    event_ts TIMESTAMP,
    event_type STRING,
    return_id STRING,
    order_id STRING,
    return_ts TIMESTAMP,
    reason_code STRING,
    event_date DATE
)
USING iceberg
PARTITIONED BY (event_date);
