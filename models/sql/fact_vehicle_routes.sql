CREATE TABLE IF NOT EXISTS iceberg.fact_vehicle_routes (
    event_id STRING,
    event_ts TIMESTAMP,
    event_type STRING,
    vehicle_id STRING,
    route_id STRING,
    assigned_ts TIMESTAMP,
    location STRING,
    event_date DATE
)
USING iceberg
PARTITIONED BY (event_date);
