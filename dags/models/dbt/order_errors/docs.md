{% docs fact_order_errors %}
The `fact_order_errors` model captures operational and system issues encountered during order processing. Each record represents an error event and is partitioned by `event_date` for efficient analysis.
{% enddocs %}

{% docs stg_order_errors %}
The `stg_order_errors` model normalizes raw order error events and computes the `event_date` partition field used by downstream models.
{% enddocs %}
