CREATE TABLE IF NOT EXISTS iceberg.fact_forecast_demand (
    event_id STRING,
    event_ts TIMESTAMP,
    event_type STRING,
    product_id STRING,
    region STRING,
    forecast_ts TIMESTAMP,
    forecast_qty INT,
    event_date DATE
)
USING iceberg
PARTITIONED BY (event_date);
