{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='warehouse_id'
  )
}}

with src as (
  select
    warehouse_id,
    cast(order_ts as date) as event_date
  from {{ source('orders', 'raw_orders') }}
  where warehouse_id is not null and warehouse_id <> ''
)

select
  warehouse_id,
  cast(null as varchar) as region,
  cast(null as varchar) as manager,
  cast(null as integer) as capacity,
  event_date
from src
