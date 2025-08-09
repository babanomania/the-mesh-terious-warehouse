{{ config(materialized='incremental', unique_key='movement_id') }}

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
{% if is_incremental() %}
where event_ts > (select max(event_ts) from {{ this }})
{% endif %}
