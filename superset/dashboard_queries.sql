-- Starter queries for Superset dashboards

-- Orders overview
SELECT warehouse_id, COUNT(*) AS total_orders
FROM fact_orders
GROUP BY warehouse_id;

-- Returns overview
SELECT warehouse_id, COUNT(*) AS total_returns
FROM fact_returns
GROUP BY warehouse_id;

-- SLA heatmap derived from dispatch logs
SELECT warehouse_id, status, COUNT(*) AS event_count
FROM fact_dispatch_logs
GROUP BY warehouse_id, status;

-- Forecast risk summary from stockout predictions
SELECT product_id,
       AVG(risk_score) AS avg_risk_score,
       AVG(confidence) AS avg_confidence
FROM fact_stockout_risks
GROUP BY product_id;
