{{ config(materialized='incremental', unique_key='return_id') }}

with source as (
    select * from {{ source('returns', 'raw_returns') }}
)

select
    event_id,
    event_ts,
    event_type,
    return_id,
    order_id,
    return_ts,
    reason_code,
    cast(return_ts as date) as event_date
from source
{% if is_incremental() %}
where return_ts > (select max(return_ts) from {{ this }})
{% endif %}
