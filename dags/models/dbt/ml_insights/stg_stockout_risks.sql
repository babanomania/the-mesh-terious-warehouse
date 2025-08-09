{{ config(materialized='view') }}

with source as (
    select * from {{ source('ml_insights', 'raw_stockout_risk') }}
)

select
    event_id,
    event_ts,
    event_type,
    product_id,
    risk_score,
    predicted_date,
    confidence,
    cast(event_ts as date) as event_date
from source
