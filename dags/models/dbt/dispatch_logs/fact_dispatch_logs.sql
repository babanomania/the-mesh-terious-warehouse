{{ config(materialized='incremental', unique_key='dispatch_id') }}

select
    event_id,
    event_ts,
    event_type,
    dispatch_id,
    order_id,
    vehicle_id,
    status,
    eta,
    event_date
from {{ ref('stg_dispatch_logs') }}
{% if is_incremental() %}
where event_ts > (select max(event_ts) from {{ this }})
{% endif %}
