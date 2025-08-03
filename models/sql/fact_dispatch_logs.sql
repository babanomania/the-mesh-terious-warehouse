CREATE TABLE IF NOT EXISTS iceberg.fact_dispatch_logs (
    event_id STRING,
    event_ts TIMESTAMP,
    event_type STRING,
    dispatch_id STRING,
    order_id STRING,
    vehicle_id STRING,
    status STRING,
    eta TIMESTAMP,
    event_date DATE
)
USING iceberg
PARTITIONED BY (event_date);
