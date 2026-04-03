-- ============================================================================
-- Bronze Schema DDL
-- Payment Risk and Order Analytics Platform
-- ============================================================================
-- File: sql/ddl/01_create_bronze_schema.sql
-- Purpose: Creates all raw bronze tables matching Olist CSV structure exactly.
--          No transformations. Faithful copy of source data.
--          Timestamps stored as VARCHAR - cast to TIMESTAMPTZ in dbt staging.
--          Column names preserved as-is including Olist typos (lenght).
-- Run: Once during Phase 3 setup. Safe to re-run (uses IF NOT EXISTS).
-- ============================================================================

-- ============================================================================
-- ORDERS
-- Grain: one row per order
-- Primary key: order_id (verified unique - 99,441 rows)
-- Notes:
--   order_approved_at has 160 nulls (unapproved orders)
--   order_delivered_carrier_date has 1,783 nulls
--   order_delivered_customer_date has 2,965 nulls
--   775 orders have no matching items (cancelled/failed orders)
--   All timestamp columns stored as VARCHAR, cast in staging
-- ============================================================================
CREATE TABLE IF NOT EXISTS bronze.orders (
    order_id                        VARCHAR(64)     NOT NULL,
    customer_id                     VARCHAR(64)     NOT NULL,
    order_status                    VARCHAR(32)     NOT NULL,
    order_purchase_timestamp        VARCHAR(32),
    order_approved_at               VARCHAR(32),
    order_delivered_carrier_date    VARCHAR(32),
    order_delivered_customer_date   VARCHAR(32),
    order_estimated_delivery_date   VARCHAR(32),
    _ingested_at                    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    _source_file                    VARCHAR(128),
    CONSTRAINT bronze_orders_pk PRIMARY KEY (order_id)
);

-- ============================================================================
-- ORDER ITEMS
-- Grain: one row per item within an order
-- Primary key: order_id + order_item_id (composite)
-- Notes:
--   No nulls in any column
--   One order can have multiple items from multiple sellers
--   Max items per order: 21 (from exploration)
--   Max sellers per order: 5 (from exploration)
--   order_item_id is a sequential integer per order (1, 2, 3...)
-- ============================================================================
CREATE TABLE IF NOT EXISTS bronze.order_items (
    order_id                VARCHAR(64)     NOT NULL,
    order_item_id           SMALLINT        NOT NULL,
    product_id              VARCHAR(64)     NOT NULL,
    seller_id               VARCHAR(64)     NOT NULL,
    shipping_limit_date     VARCHAR(32),
    price                   DECIMAL(10,2)   NOT NULL,
    freight_value           DECIMAL(10,2)   NOT NULL,
    _ingested_at            TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    _source_file            VARCHAR(128),
    CONSTRAINT bronze_order_items_pk PRIMARY KEY (order_id, order_item_id)
);

-- ============================================================================
-- ORDER PAYMENTS
-- Grain: one row per payment transaction per order
-- Primary key: order_id + payment_sequential (composite)
-- Notes:
--   No nulls in any column
--   One order can have multiple payment rows (installments or split methods)
--   payment_sequential is the installment sequence number (1, 2, 3...)
--   payment_type includes: credit_card, boleto, voucher, debit_card, not_defined
--   not_defined has 3 rows - loaded as-is, handled in dbt staging
--   payment_installments = 0 exists for some voucher payments
-- ============================================================================
CREATE TABLE IF NOT EXISTS bronze.order_payments (
    order_id                VARCHAR(64)     NOT NULL,
    payment_sequential      SMALLINT        NOT NULL,
    payment_type            VARCHAR(32)     NOT NULL,
    payment_installments    SMALLINT        NOT NULL,
    payment_value           DECIMAL(10,2)   NOT NULL,
    _ingested_at            TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    _source_file            VARCHAR(128),
    CONSTRAINT bronze_order_payments_pk PRIMARY KEY (order_id, payment_sequential)
);

-- ============================================================================
-- CUSTOMERS
-- Grain: one row per customer_id (per-order customer record)
-- Primary key: customer_id (verified unique - 99,441 rows)
-- Notes:
--   No nulls in any column
--   customer_unique_id identifies real unique customers (96,096 unique)
--   customer_id is per-order - same real customer gets new customer_id per order
--   dim_customer will use customer_unique_id as natural key, not customer_id
--   customer_zip_code_prefix stored as VARCHAR to preserve leading zeros
-- ============================================================================
CREATE TABLE IF NOT EXISTS bronze.customers (
    customer_id                 VARCHAR(64)     NOT NULL,
    customer_unique_id          VARCHAR(64)     NOT NULL,
    customer_zip_code_prefix    VARCHAR(10)     NOT NULL,
    customer_city               VARCHAR(128),
    customer_state              VARCHAR(4),
    _ingested_at                TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    _source_file                VARCHAR(128),
    CONSTRAINT bronze_customers_pk PRIMARY KEY (customer_id)
);

-- ============================================================================
-- SELLERS
-- Grain: one row per seller
-- Primary key: seller_id (verified unique - 3,095 rows)
-- Notes:
--   No nulls in any column
--   seller_zip_code_prefix stored as VARCHAR to preserve leading zeros
--   Seller state heavily concentrated in SP (1,849 of 3,095)
--   No explicit onboarding date - MIN(order_purchase_timestamp)
--   used as proxy in dbt intermediate models
-- ============================================================================
CREATE TABLE IF NOT EXISTS bronze.sellers (
    seller_id                   VARCHAR(64)     NOT NULL,
    seller_zip_code_prefix      VARCHAR(10)     NOT NULL,
    seller_city                 VARCHAR(128),
    seller_state                VARCHAR(4),
    _ingested_at                TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    _source_file                VARCHAR(128),
    CONSTRAINT bronze_sellers_pk PRIMARY KEY (seller_id)
);

-- ============================================================================
-- PRODUCTS
-- Grain: one row per product
-- Primary key: product_id (verified unique - 32,951 rows)
-- Notes:
--   product_category_name has 610 nulls
--   product_name_lenght and product_description_lenght are Olist typos
--   Column names preserved as-is in bronze, renamed in dbt staging
--   2 categories missing English translation (pc_gamer,
--   portateis_cozinha_e_preparadores_de_alimentos) - handled with
--   COALESCE in dbt staging, falls back to Portuguese name
--   English category names applied from dbt seed in staging onwards
-- ============================================================================
CREATE TABLE IF NOT EXISTS bronze.products (
    product_id                      VARCHAR(64)     NOT NULL,
    product_category_name           VARCHAR(128),
    product_name_lenght             DECIMAL(6,1),
    product_description_lenght      DECIMAL(8,1),
    product_photos_qty              DECIMAL(4,1),
    product_weight_g                DECIMAL(10,1),
    product_length_cm               DECIMAL(6,1),
    product_height_cm               DECIMAL(6,1),
    product_width_cm                DECIMAL(6,1),
    _ingested_at                    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    _source_file                    VARCHAR(128),
    CONSTRAINT bronze_products_pk PRIMARY KEY (product_id)
);

-- ============================================================================
-- REVIEWS
-- Grain: one row per review submission
-- Primary key: review_id + order_id (composite - review_id has 814 duplicates)
-- Notes:
--   review_id is NOT unique (814 duplicates found in exploration)
--   Composite key of review_id + order_id used as bronze PK
--   review_comment_title has 87,656 nulls (most reviews have no title)
--   review_comment_message has 58,247 nulls (most reviews have no message)
--   In dbt staging: deduplicate by order_id keeping latest review_creation_date
--   543 orders have 2 reviews, 4 orders have 3 reviews
--   review_score range: 1-5 (verified in exploration)
-- ============================================================================
CREATE TABLE IF NOT EXISTS bronze.reviews (
    review_id                   VARCHAR(64)     NOT NULL,
    order_id                    VARCHAR(64)     NOT NULL,
    review_score                SMALLINT        NOT NULL,
    review_comment_title        TEXT,
    review_comment_message      TEXT,
    review_creation_date        VARCHAR(32),
    review_answer_timestamp     VARCHAR(32),
    _ingested_at                TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    _source_file                VARCHAR(128),
    CONSTRAINT bronze_reviews_pk PRIMARY KEY (review_id, order_id)
);

-- ============================================================================
-- GEOLOCATION
-- Grain: multiple rows per zip code prefix (average used in dbt staging)
-- No primary key - intentionally not unique (1,000,163 rows, 19,015 zip codes)
-- Notes:
--   No nulls in any column
--   Average lat/lng computed per zip in dbt staging to get one row per zip
--   Used to compute geographic distance between seller and customer
--   Joined to customers and sellers via zip_code_prefix
-- ============================================================================
CREATE TABLE IF NOT EXISTS bronze.geolocation (
    geolocation_zip_code_prefix     VARCHAR(10)     NOT NULL,
    geolocation_lat                 DECIMAL(10,6)   NOT NULL,
    geolocation_lng                 DECIMAL(10,6)   NOT NULL,
    geolocation_city                VARCHAR(128),
    geolocation_state               VARCHAR(4),
    _ingested_at                    TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    _source_file                    VARCHAR(128)
);

CREATE INDEX IF NOT EXISTS idx_bronze_geolocation_zip
    ON bronze.geolocation (geolocation_zip_code_prefix);

-- ============================================================================
-- INDEXES FOR COMMON JOIN PATTERNS
-- ============================================================================

-- Orders joined to items, payments, reviews via order_id
CREATE INDEX IF NOT EXISTS idx_bronze_order_items_order_id
    ON bronze.order_items (order_id);

CREATE INDEX IF NOT EXISTS idx_bronze_order_payments_order_id
    ON bronze.order_payments (order_id);

CREATE INDEX IF NOT EXISTS idx_bronze_reviews_order_id
    ON bronze.reviews (order_id);

-- Customers joined via customer_unique_id for real customer deduplication
CREATE INDEX IF NOT EXISTS idx_bronze_customers_unique_id
    ON bronze.customers (customer_unique_id);

-- Products joined via product_id from order_items
CREATE INDEX IF NOT EXISTS idx_bronze_order_items_product_id
    ON bronze.order_items (product_id);

-- Sellers joined via seller_id from order_items
CREATE INDEX IF NOT EXISTS idx_bronze_order_items_seller_id
    ON bronze.order_items (seller_id);

-- ============================================================================
-- VERIFICATION QUERY
-- Run after loader completes to confirm expected row counts
-- ============================================================================

-- SELECT
--     'orders'         AS table_name, COUNT(*) AS row_count FROM bronze.orders
-- UNION ALL SELECT
--     'order_items',   COUNT(*) FROM bronze.order_items
-- UNION ALL SELECT
--     'order_payments', COUNT(*) FROM bronze.order_payments
-- UNION ALL SELECT
--     'customers',     COUNT(*) FROM bronze.customers
-- UNION ALL SELECT
--     'sellers',       COUNT(*) FROM bronze.sellers
-- UNION ALL SELECT
--     'products',      COUNT(*) FROM bronze.products
-- UNION ALL SELECT
--     'reviews',       COUNT(*) FROM bronze.reviews
-- UNION ALL SELECT
--     'geolocation',   COUNT(*) FROM bronze.geolocation;

-- Expected counts:
--   orders:          99,441
--   order_items:    112,650
--   order_payments: 103,886
--   customers:       99,441
--   sellers:          3,095
--   products:        32,951
--   reviews:         99,224
--   geolocation:  1,000,163
