CREATE OR REPLACE VIEW vw_sales_by_category AS
SELECT
  category,
  COUNT(DISTINCT order_id)   AS total_orders,
  SUM(quantity)              AS total_units_sold,
  ROUND(SUM(revenue), 2)     AS total_revenue,
  ROUND(SUM(profit), 2)      AS total_profit,
  ROUND(AVG(unit_price), 2)  AS avg_unit_price
FROM orders
WHERE category != 'Unknown'
GROUP BY category
ORDER BY total_revenue DESC;

CREATE OR REPLACE VIEW vw_top_customers AS
SELECT
  customer_id,
  customer_name,
  email,
  COUNT(DISTINCT order_id)  AS total_orders,
  SUM(quantity)             AS total_units,
  ROUND(SUM(revenue), 2)    AS total_revenue,
  ROUND(SUM(profit), 2)     AS total_profit
FROM orders
WHERE customer_name != 'Unknown'
GROUP BY customer_id, customer_name, email
ORDER BY total_revenue DESC;

CREATE OR REPLACE VIEW vw_order_status AS
SELECT
  status,
  COUNT(DISTINCT order_id)  AS total_orders,
  ROUND(SUM(revenue), 2)    AS total_revenue
FROM orders
GROUP BY status
ORDER BY total_orders DESC;

CREATE OR REPLACE VIEW vw_sales_by_country AS
SELECT
  country,
  COUNT(DISTINCT order_id)  AS total_orders,
  ROUND(SUM(revenue), 2)    AS total_revenue,
  ROUND(SUM(profit), 2)     AS total_profit
FROM orders
WHERE country != 'Unknown'
GROUP BY country
ORDER BY total_revenue DESC;

CREATE OR REPLACE VIEW vw_daily_revenue AS
SELECT
  DATE(order_timestamp)      AS order_date,
  COUNT(DISTINCT order_id)   AS total_orders,
  ROUND(SUM(revenue), 2)     AS total_revenue,
  ROUND(SUM(profit), 2)      AS total_profit
FROM orders
GROUP BY DATE(order_timestamp)
ORDER BY order_date DESC;
