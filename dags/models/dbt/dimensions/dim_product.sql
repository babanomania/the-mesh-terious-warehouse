{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='product_id'
  )
}}

with src as (
  select
    product_id,
    cast(order_ts as date) as event_date
  from {{ source('orders', 'raw_orders') }}
  where product_id is not null and product_id <> ''
)

select
  product_id,
  cast(null as varchar) as name,
  cast(null as varchar) as category,
  cast(null as integer) as unit_cost,
  event_date
from src
