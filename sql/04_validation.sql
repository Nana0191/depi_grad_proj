-- =====================================================================
-- 04_validation.sql
-- Checks that the star schema load is complete and consistent.
-- =====================================================================
USE OlistDW;
GO

-- 1. Row counts across dimensions and fact table
SELECT 'dim_customers' AS table_name, COUNT(*) AS rows_loaded FROM dim_customers
UNION ALL SELECT 'dim_products',  COUNT(*) FROM dim_products
UNION ALL SELECT 'dim_sellers',   COUNT(*) FROM dim_sellers
UNION ALL SELECT 'dim_date',      COUNT(*) FROM dim_date
UNION ALL SELECT 'fact_sales',    COUNT(*) FROM fact_sales;
GO

-- 2. Revenue and item total checks
SELECT SUM(price)         AS sum_price,
       SUM(freight_value) AS sum_freight,
       SUM(item_total)    AS sum_item_total
FROM fact_sales;
GO

-- 3. Orphan / referential integrity checks (expect 0 for every check)
SELECT COUNT(*) AS sales_without_customer
FROM fact_sales s
LEFT JOIN dim_customers c ON c.customer_key = s.customer_key
WHERE c.customer_key IS NULL;

SELECT COUNT(*) AS sales_without_product
FROM fact_sales s
LEFT JOIN dim_products p ON p.product_key = s.product_key
WHERE p.product_key IS NULL;

SELECT COUNT(*) AS sales_without_seller
FROM fact_sales s
LEFT JOIN dim_sellers sl ON sl.seller_key = s.seller_key
WHERE sl.seller_key IS NULL;

SELECT COUNT(*) AS sales_without_purchase_date
FROM fact_sales s
LEFT JOIN dim_date d ON d.date_key = s.purchase_date_key
WHERE d.date_key IS NULL;

SELECT COUNT(*) AS sales_with_invalid_delivered_date
FROM fact_sales s
LEFT JOIN dim_date d ON d.date_key = s.delivered_date_key
WHERE s.delivered_date_key IS NOT NULL
  AND d.date_key IS NULL;
GO

-- 4. Orders with no items (placeholder rows with order_item_id = 0)
SELECT COUNT(*) AS orders_without_items
FROM fact_sales
WHERE order_item_id = 0;
GO