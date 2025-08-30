{{ config(materialized='incremental', unique_key='error_id') }}

select
    event_id,
    event_ts,
    event_type,
    error_id,
    order_id,
    error_code,
    detected_ts,
    event_date
from {{ ref('stg_order_errors') }}
{% if is_incremental() %}
where detected_ts > (select max(detected_ts) from {{ this }})
{% endif %}
