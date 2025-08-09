{{ config(materialized='incremental', unique_key='event_id') }}

select
    event_id,
    event_ts,
    event_type,
    product_id,
    region,
    forecast_ts,
    forecast_qty,
    event_date
from {{ ref('stg_forecast_demand') }}
{% if is_incremental() %}
where forecast_ts > (select max(forecast_ts) from {{ this }})
{% endif %}
