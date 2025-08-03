{% docs fact_dispatch_logs %}
The `fact_dispatch_logs` model consolidates dispatch log events into an analytics-ready fact table. Each row captures a dispatch event, including vehicle assignment, status updates, and estimated arrival times. Records are partitioned by `event_date` for efficient querying.
{% enddocs %}

{% docs stg_dispatch_logs %}
The `stg_dispatch_logs` model normalizes raw dispatch log events and derives the `event_date` partition field used by downstream models.
{% enddocs %}
