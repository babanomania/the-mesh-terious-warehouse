{{ config(materialized='view') }}

with source as (
    select * from {{ source('orders', 'raw_orders') }}
)

select
    event_id,
    event_ts,
    event_type,
    order_id,
    product_id,
    warehouse_id,
    order_ts,
    qty,
    cast(order_ts as date) as event_date
from source
