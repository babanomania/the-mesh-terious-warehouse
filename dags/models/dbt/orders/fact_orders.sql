{{ config(materialized='incremental', unique_key='order_id') }}

select
    event_id,
    event_ts,
    event_type,
    order_id,
    product_id,
    warehouse_id,
    order_ts,
    qty,
    event_date
from {{ ref('stg_orders') }}
{% if is_incremental() %}
where order_ts > (select max(order_ts) from {{ this }})
{% endif %}
