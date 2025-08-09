CREATE TABLE IF NOT EXISTS iceberg.dim_employee (
    employee_id STRING,
    role STRING,
    assigned_warehouse STRING,
    shift_hours INT,
    event_date DATE
)
USING iceberg
PARTITIONED BY (event_date);
