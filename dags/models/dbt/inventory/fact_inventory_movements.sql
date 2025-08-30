{{ config(materialized='incremental', unique_key='movement_id') }}

select
    event_id,
    event_ts,
    event_type,
    movement_id,
    product_id,
    delta_qty,
    source_type,
    event_date
from {{ ref('stg_inventory_movements') }}
{% if is_incremental() %}
where event_ts > (select max(event_ts) from {{ this }})
{% endif %}
