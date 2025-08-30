{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='date_id'
  )
}}

with dates as (
  select distinct cast(order_ts as date) as date_id
  from {{ source('orders', 'raw_orders') }}
  union all
  select distinct cast(return_ts as date) as date_id
  from {{ source('returns', 'raw_returns') }}
  union all
  select distinct cast(event_ts as date) as date_id
  from {{ source('dispatch_logs', 'raw_dispatch_logs') }}
)
select
  date_id,
  extract(day from date_id)::int as day,
  strftime(date_id, '%W')::int as week,
  extract(month from date_id)::int as month,
  ((extract(month from date_id)::int - 1) / 3 + 1)::int as quarter,
  extract(year from date_id)::int as year,
  date_id as event_date
from dates
where date_id is not null
