{{ config(materialized='incremental', unique_key='dispatch_id') }}

with source as (
    select * from {{ source('dispatch_logs', 'raw_dispatch_logs') }}
)

select
    event_id,
    event_ts,
    event_type,
    dispatch_id,
    order_id,
    vehicle_id,
    status,
    eta,
    cast(event_ts as date) as event_date
from source
{% if is_incremental() %}
where event_ts > (select max(event_ts) from {{ this }})
{% endif %}
