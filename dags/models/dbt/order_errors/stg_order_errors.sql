{{ config(materialized='view') }}

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

