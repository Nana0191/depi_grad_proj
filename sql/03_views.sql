-- =====================================================================
-- 03_views.sql
-- Analytical views for reporting against the fact_sales star schema.
-- Run after data is loaded.
-- =====================================================================
USE OlistDW;
GO

-- Revenue and items sold by product category and purchase month
CREATE OR ALTER VIEW vw_sales_by_category_month AS
SELECT p.product_category_name_english AS category,
       d.year,
       d.month,
       SUM(CASE WHEN s.order_item_id > 0 THEN 1 ELSE 0 END) AS items_sold,
       SUM(s.item_total)                                     AS revenue
FROM fact_sales s
JOIN dim_products p ON p.product_key = s.product_key
JOIN dim_date d     ON d.date_key = s.purchase_date_key
GROUP BY p.product_category_name_english, d.year, d.month;
GO

-- Late delivery rate by customer state (deduplicated to 1 row per order)
CREATE OR ALTER VIEW vw_late_delivery_by_state AS
WITH dedup_orders AS (
    SELECT c.customer_state,
           s.is_late,
           ROW_NUMBER() OVER (PARTITION BY s.order_id ORDER BY s.order_item_id ASC) AS rn
    FROM fact_sales s
    JOIN dim_customers c ON c.customer_key = s.customer_key
    WHERE s.is_late IS NOT NULL
)
SELECT customer_state,
       COUNT(*)                                              AS delivered_orders,
       CAST(SUM(CAST(is_late AS int)) AS decimal(12,6)) / COUNT(*) AS late_rate
FROM dedup_orders
WHERE rn = 1
GROUP BY customer_state;
GO

-- Items sold and revenue per seller (excludes placeholder rows)
CREATE OR ALTER VIEW vw_items_by_seller AS
SELECT sl.seller_id,
       sl.seller_state,
       COUNT(*)          AS items_sold,
       SUM(s.item_total) AS revenue
FROM fact_sales s
JOIN dim_sellers sl ON sl.seller_key = s.seller_key
WHERE s.order_item_id > 0
GROUP BY sl.seller_id, sl.seller_state;
GO