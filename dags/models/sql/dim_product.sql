CREATE TABLE IF NOT EXISTS iceberg.dim_product (
    product_id STRING,
    name STRING,
    category STRING,
    unit_cost DECIMAL(10,2),
    event_date DATE
)
USING iceberg
PARTITIONED BY (event_date);
