CREATE TABLE IF NOT EXISTS iceberg.dim_error_code (
    error_code STRING,
    description STRING,
    severity_level STRING,
    event_date DATE
)
USING iceberg
PARTITIONED BY (event_date);
