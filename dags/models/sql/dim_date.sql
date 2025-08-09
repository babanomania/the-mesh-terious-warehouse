CREATE TABLE IF NOT EXISTS iceberg.dim_date (
    date_id DATE,
    day INT,
    week INT,
    month INT,
    quarter INT,
    year INT,
    event_date DATE
)
USING iceberg
PARTITIONED BY (event_date);
