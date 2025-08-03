{{ config(materialized='view') }}

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

