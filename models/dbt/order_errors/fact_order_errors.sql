{{ config(materialized='incremental', unique_key='error_id') }}

with source as (
    select * from {{ source('order_errors', 'raw_order_errors') }}
)

select
    event_id,
    event_ts,
    event_type,
    error_id,
    order_id,
    error_code,
    detected_ts,
    cast(detected_ts as date) as event_date
from source
{% if is_incremental() %}
where detected_ts > (select max(detected_ts) from {{ this }})
{% endif %}
