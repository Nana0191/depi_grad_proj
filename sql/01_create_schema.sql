-- =====================================================================
-- 01_create_schema.sql
-- Olist star schema for SQL Server: ONE fact table, FOUR dimensions.
--
--                 dim_date
--                    |
--   dim_customers - fact_sales - dim_products
--                    |
--                dim_sellers
--
-- fact_sales grain: one row per order item (order_item_id >= 1), plus
-- one placeholder row (order_item_id = 0, product_key = -1,
-- seller_key = -1, amounts = 0.00) for orders that have no items.
--
-- Re-runnable: drops and recreates all objects (this deletes data).
-- Run order afterwards: 02 -> load -> 03 -> 04.
-- =====================================================================

IF DB_ID('OlistDW') IS NULL
    CREATE DATABASE OlistDW;
GO
USE OlistDW;
GO

-- ---------- Clean slate (views, fact, then dimensions) ----------
-- removed the drop tables and view if exists bec this sql scripts needs to be
-- safe not to lose our data and these sql scripts will only run once
-- ---------- Dimensions ----------

-- One row per real customer (customer_unique_id).
-- Address attributes: Type 1 (latest order's address overwrites older ones).
CREATE TABLE dim_customers (
    customer_key             int          IDENTITY(1,1) NOT NULL CONSTRAINT pk_dim_customers PRIMARY KEY,
    customer_unique_id       char(32)     NOT NULL CONSTRAINT uq_dim_customers_uid UNIQUE,
    customer_zip_code_prefix char(5)      NULL,
    customer_city            varchar(50)  NULL,
    customer_state           char(2)      NULL,
    loaded_at                datetime2    NOT NULL CONSTRAINT df_dim_customers_loaded DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE dim_products (
    product_key                   int         IDENTITY(1,1) NOT NULL CONSTRAINT pk_dim_products PRIMARY KEY,
    product_id                    char(32)    NOT NULL CONSTRAINT uq_dim_products_id UNIQUE,
    product_category_name         varchar(50) NULL,
    product_category_name_english varchar(50) NULL,
    category_status               varchar(20) NOT NULL,
    product_name_length           int         NULL,
    product_description_length    int         NULL,
    product_photos_qty            int         NULL,
    product_weight_g              int         NULL,
    product_length_cm             int         NULL,
    product_height_cm             int         NULL,
    product_width_cm              int         NULL,
    has_missing_dimensions        bit         NOT NULL,
    volume_cm3                    int         NULL,
    loaded_at                     datetime2   NOT NULL CONSTRAINT df_dim_products_loaded DEFAULT SYSUTCDATETIME()
);
GO

-- Attributes are nullable: if no sellers file is available, the loader
-- fills seller_id from the distinct values in the items data.
CREATE TABLE dim_sellers (
    seller_key             int         IDENTITY(1,1) NOT NULL CONSTRAINT pk_dim_sellers PRIMARY KEY,
    seller_id              char(32)    NOT NULL CONSTRAINT uq_dim_sellers_id UNIQUE,
    seller_zip_code_prefix char(5)     NULL,
    seller_city            varchar(50) NULL,
    seller_state           char(2)     NULL,
    loaded_at              datetime2   NOT NULL CONSTRAINT df_dim_sellers_loaded DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE dim_date (
    date_key     int         NOT NULL CONSTRAINT pk_dim_date PRIMARY KEY,  -- YYYYMMDD
    full_date    date        NOT NULL CONSTRAINT uq_dim_date_full UNIQUE,
    year         smallint    NOT NULL,
    quarter      tinyint     NOT NULL,
    month        tinyint     NOT NULL,
    day          tinyint     NOT NULL,
    weekday_name varchar(10) NOT NULL,
    is_weekend   bit         NOT NULL
);
GO

-- ---------- "Unknown" members for orders that have no items ----------
-- Key -1 sits outside the identity range (which starts at 1). The loader
-- must not delete these rows (e.g., use DELETE ... WHERE product_key > 0).
SET IDENTITY_INSERT dim_products ON;
INSERT INTO dim_products (
    product_key, product_id, product_category_name,
    product_category_name_english, category_status, has_missing_dimensions
)
VALUES (-1, 'UNKNOWN', 'no_item', 'no_item', 'unknown', 1);
SET IDENTITY_INSERT dim_products OFF;

SET IDENTITY_INSERT dim_sellers ON;
INSERT INTO dim_sellers (seller_key, seller_id)
VALUES (-1, 'UNKNOWN');
SET IDENTITY_INSERT dim_sellers OFF;
GO

-- ---------- Fact ----------

-- Grain: one row per order item (order_item_id >= 1), plus one row with
-- order_item_id = 0 for each order that has no items.
-- order_id is a degenerate dimension (no dimension table, no FK).
--
-- Order-level columns (order_status, timestamps, approval_hours,
-- delivery_days, delivery_delay_days, is_late, delivered_date_key)
-- repeat across items of the same order; use order-level deduplication
-- (e.g. ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_item_id ASC))
-- when aggregating order-level metrics.
-- Item-level columns (price, freight_value, item_total) are safe to SUM.
CREATE TABLE fact_sales (
    order_id                      char(32)      NOT NULL,
    order_item_id                 int           NOT NULL,   -- 0 = order has no items
    customer_key                  int           NOT NULL,
    product_key                   int           NOT NULL,
    seller_key                    int           NOT NULL,
    purchase_date_key             int           NOT NULL,
    delivered_date_key            int           NULL,       -- role-playing use of dim_date
    -- item measures
    price                         decimal(12,2) NOT NULL,
    freight_value                 decimal(12,2) NOT NULL,
    item_total                    decimal(12,2) NOT NULL,
    shipping_limit_date           datetime2     NULL,
    -- order-level attributes and measures
    order_status                  varchar(20)   NULL,
    order_purchase_timestamp      datetime2     NULL,
    order_approved_at             datetime2     NULL,
    order_delivered_carrier_date  datetime2     NULL,
    order_delivered_customer_date datetime2     NULL,
    order_estimated_delivery_date date          NULL,
    approval_hours                decimal(10,2) NULL,
    delivery_days                 int           NULL,
    delivery_delay_days           int           NULL,
    is_late                       bit           NULL,
    loaded_at                     datetime2     NOT NULL CONSTRAINT df_fact_sales_loaded DEFAULT SYSUTCDATETIME(),
    CONSTRAINT pk_fact_sales PRIMARY KEY (order_id, order_item_id),
    CONSTRAINT ck_fact_sales_total CHECK (ABS(item_total - (price + freight_value)) <= 0.01),
    CONSTRAINT ck_fact_sales_item  CHECK (order_item_id >= 0),
    CONSTRAINT fk_sales_customer  FOREIGN KEY (customer_key)       REFERENCES dim_customers(customer_key),
    CONSTRAINT fk_sales_product   FOREIGN KEY (product_key)        REFERENCES dim_products(product_key),
    CONSTRAINT fk_sales_seller    FOREIGN KEY (seller_key)         REFERENCES dim_sellers(seller_key),
    CONSTRAINT fk_sales_purchase  FOREIGN KEY (purchase_date_key)  REFERENCES dim_date(date_key),
    CONSTRAINT fk_sales_delivered FOREIGN KEY (delivered_date_key) REFERENCES dim_date(date_key)
);
GO

-- ---------- Indexes on frequently joined / filtered columns ----------
CREATE INDEX ix_sales_customer  ON fact_sales(customer_key);
CREATE INDEX ix_sales_product   ON fact_sales(product_key);
CREATE INDEX ix_sales_seller    ON fact_sales(seller_key);
CREATE INDEX ix_sales_purchase  ON fact_sales(purchase_date_key);
CREATE INDEX ix_sales_delivered ON fact_sales(delivered_date_key);
CREATE INDEX ix_sales_status    ON fact_sales(order_status);

CREATE INDEX ix_products_cat    ON dim_products(product_category_name_english);
CREATE INDEX ix_customers_state ON dim_customers(customer_state);
CREATE INDEX ix_sellers_state   ON dim_sellers(seller_state);
GO