{% docs fact_dispatch_logs %}
The `fact_dispatch_logs` model consolidates dispatch log events into an analytics-ready fact table. Each row captures a dispatch event, including vehicle assignment, status updates, and estimated arrival times. Records are partitioned by `event_date` for efficient querying.
{% enddocs %}
