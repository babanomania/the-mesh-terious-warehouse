{% docs stg_inventory_movements %}
The `stg_inventory_movements` model normalizes raw inventory movement events and computes the `event_date` partition field used by downstream models.
{% enddocs %}

{% docs fact_inventory_movements %}
Records changes to product inventory levels. Each row represents a single movement event captured from warehouse operations.
{% enddocs %}
