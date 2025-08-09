CREATE TABLE IF NOT EXISTS iceberg.dim_route (
    route_id STRING,
    region_covered STRING,
    avg_duration INT,
    event_date DATE
)
USING iceberg
PARTITIONED BY (event_date);
