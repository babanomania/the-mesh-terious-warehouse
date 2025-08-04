{% docs stg_forecast_demand %}
The `stg_forecast_demand` model normalizes raw demand forecast events and derives the `event_date` partition column.
{% enddocs %}

{% docs fact_forecast_demand %}
Stores demand forecast outputs per product and region for downstream analytics.
{% enddocs %}

{% docs stg_stockout_risks %}
The `stg_stockout_risks` model prepares stockout risk prediction events and computes the `event_date` partition column.
{% enddocs %}

{% docs fact_stockout_risks %}
Captures ML-predicted stockout risk scores with associated confidence levels.
{% enddocs %}
