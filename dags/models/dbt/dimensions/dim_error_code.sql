{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='error_code'
  )
}}

with src as (
  select
    reason_code as error_code,
    cast(return_ts as date) as event_date
  from {{ source('returns', 'raw_returns') }}
  where reason_code is not null and reason_code <> ''
)

select
  error_code,
  cast(null as varchar) as description,
  cast(null as varchar) as severity_level,
  event_date
from src
