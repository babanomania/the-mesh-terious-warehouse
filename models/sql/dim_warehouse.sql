CREATE TABLE IF NOT EXISTS iceberg.dim_warehouse (
    warehouse_id STRING,
    region STRING,
    manager STRING,
    capacity INT,
    event_date DATE
)
USING iceberg
PARTITIONED BY (event_date);
