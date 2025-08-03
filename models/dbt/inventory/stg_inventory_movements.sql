{{ config(materialized='view') }}

with source as (
    select * from {{ source('inventory', 'raw_inventory_movements') }}
)

select
    event_id,
    event_ts,
    event_type,
    movement_id,
    product_id,
    delta_qty,
    source_type,
    cast(event_ts as date) as event_date
from source
