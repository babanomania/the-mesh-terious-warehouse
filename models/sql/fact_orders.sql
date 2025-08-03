CREATE TABLE IF NOT EXISTS iceberg.fact_orders (
    event_id STRING,
    event_ts TIMESTAMP,
    event_type STRING,
    order_id STRING,
    product_id STRING,
    warehouse_id STRING,
    order_ts TIMESTAMP,
    qty INT,
    event_date DATE
)
USING iceberg
PARTITIONED BY (event_date);
