CREATE TABLE IF NOT EXISTS iceberg.dim_vehicle (
    vehicle_id STRING,
    type STRING,
    capacity INT,
    current_location STRING,
    event_date DATE
)
USING iceberg
PARTITIONED BY (event_date);
