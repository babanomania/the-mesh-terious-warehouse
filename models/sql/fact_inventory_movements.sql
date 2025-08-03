CREATE TABLE IF NOT EXISTS iceberg.fact_inventory_movements (
    event_id STRING,
    event_ts TIMESTAMP,
    event_type STRING,
    movement_id STRING,
    product_id STRING,
    delta_qty INT,
    source_type STRING,
    event_date DATE
)
USING iceberg
PARTITIONED BY (event_date);
