{{ config(materialized='incremental', unique_key='return_id') }}

select
    event_id,
    event_ts,
    event_type,
    return_id,
    order_id,
    return_ts,
    reason_code,
    event_date
from {{ ref('stg_returns') }}
{% if is_incremental() %}
where return_ts > (select max(return_ts) from {{ this }})
{% endif %}

