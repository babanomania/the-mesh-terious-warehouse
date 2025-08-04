{{ config(materialized='incremental', unique_key='event_id') }}

select
    event_id,
    event_ts,
    event_type,
    product_id,
    risk_score,
    predicted_date,
    confidence,
    event_date
from {{ ref('stg_stockout_risks') }}
{% if is_incremental() %}
where event_ts > (select max(event_ts) from {{ this }})
{% endif %}
