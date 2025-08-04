{{ config(materialized='view') }}

with source as (
    select * from {{ source('ml_insights', 'raw_demand_forecast') }}
)

select
    event_id,
    event_ts,
    event_type,
    product_id,
    region,
    forecast_ts,
    forecast_qty,
    cast(forecast_ts as date) as event_date
from source
