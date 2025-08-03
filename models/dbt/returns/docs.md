{% docs fact_returns %}
The `fact_returns` model curates return events into an analytics-ready fact table. Each row represents a returned order event and is partitioned by `event_date` for efficient querying.
{% enddocs %}

{% docs stg_returns %}
The `stg_returns` model normalizes raw return events and computes the `event_date` partition field used by downstream models.
{% enddocs %}
