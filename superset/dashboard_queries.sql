-- Curated Superset queries based on er_diagram.mmd

-- Orders by warehouse
SELECT warehouse_id, COUNT(*) AS total_orders
FROM orders.fact_orders
GROUP BY warehouse_id
ORDER BY total_orders DESC;

-- Orders by product
SELECT product_id, COUNT(*) AS total_orders
FROM orders.fact_orders
GROUP BY product_id
ORDER BY total_orders DESC;

-- Daily orders
SELECT event_date, COUNT(*) AS total_orders
FROM orders.fact_orders
GROUP BY event_date
ORDER BY event_date;

-- Returns by warehouse (via orders)
SELECT o.warehouse_id, COUNT(*) AS total_returns
FROM returns.fact_returns r
JOIN orders.fact_orders o ON r.order_id = o.order_id
GROUP BY o.warehouse_id
ORDER BY total_returns DESC;

-- Returns by reason
SELECT reason_code, COUNT(*) AS total_returns
FROM returns.fact_returns
GROUP BY reason_code
ORDER BY total_returns DESC;

-- Daily returns
SELECT event_date, COUNT(*) AS total_returns
FROM returns.fact_returns
GROUP BY event_date
ORDER BY event_date;

-- Inventory movements by product (sum of quantity deltas)
SELECT product_id, SUM(delta_qty) AS total_delta_qty
FROM inventory.fact_inventory_movements
GROUP BY product_id
ORDER BY total_delta_qty DESC;

-- Daily inventory movement (sum of quantity deltas)
SELECT event_date, SUM(delta_qty) AS total_delta_qty
FROM inventory.fact_inventory_movements
GROUP BY event_date
ORDER BY event_date;

-- Dispatch events by status
SELECT status, COUNT(*) AS event_count
FROM dispatch_logs.fact_dispatch_logs
GROUP BY status
ORDER BY event_count DESC;

-- Dispatch events by vehicle and status (for heatmap)
SELECT vehicle_id, status, COUNT(*) AS event_count
FROM dispatch_logs.fact_dispatch_logs
GROUP BY vehicle_id, status
ORDER BY event_count DESC;
