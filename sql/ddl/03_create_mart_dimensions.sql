-- ============================================================================
-- Mart Dimensions DDL
-- Payment Risk and Order Analytics Platform
-- ============================================================================
-- File: sql/ddl/03_create_mart_dimensions.sql
-- Purpose: Creates all dimension tables in the snowflake schema.
--          Tables created in dependency order - parent before child.
--          SCD2 columns on dim_customer and dim_seller only.
--          Surrogate keys throughout - natural keys stored separately.
-- Run: Once during Phase 3 setup via ingestion/setup_warehouse.py
-- Safe to re-run: yes (uses IF NOT EXISTS)
-- ============================================================================

-- ============================================================================
-- GEOGRAPHY HIERARCHY
-- Chain: dim_region <- dim_state <- dim_city <- dim_geo
-- Olist data is Brazil-only but structure supports future expansion
-- ============================================================================

CREATE TABLE IF NOT EXISTS mart.dim_region (
    region_sk           SERIAL          PRIMARY KEY,
    region_code         VARCHAR(20)     NOT NULL,
    region_name         VARCHAR(100)    NOT NULL,
    is_high_risk_region BOOLEAN         NOT NULL DEFAULT false,
    UNIQUE (region_code)
);

CREATE TABLE IF NOT EXISTS mart.dim_state (
    state_sk            SERIAL          PRIMARY KEY,
    region_sk           INTEGER         NOT NULL REFERENCES mart.dim_region(region_sk),
    state_code          VARCHAR(4)      NOT NULL,
    state_name          VARCHAR(100),
    UNIQUE (state_code)
);

CREATE TABLE IF NOT EXISTS mart.dim_city (
    city_sk             SERIAL          PRIMARY KEY,
    state_sk            INTEGER         NOT NULL REFERENCES mart.dim_state(state_sk),
    city_name           VARCHAR(128)    NOT NULL,
    UNIQUE (state_sk, city_name)
);

CREATE TABLE IF NOT EXISTS mart.dim_geo (
    geo_sk                      SERIAL          PRIMARY KEY,
    city_sk                     INTEGER         NOT NULL REFERENCES mart.dim_city(city_sk),
    zip_code_prefix             VARCHAR(10)     NOT NULL,
    avg_lat                     DECIMAL(10,6),
    avg_lng                     DECIMAL(10,6),
    UNIQUE (zip_code_prefix)
);

-- ============================================================================
-- CUSTOMER HIERARCHY
-- Chain: dim_customer_risk_tier <- dim_customer_segment <- dim_customer
-- SCD2 on dim_customer: new record created when segment or risk tier changes
-- Natural key: customer_unique_id (not customer_id which is per-order)
-- ============================================================================

CREATE TABLE IF NOT EXISTS mart.dim_customer_risk_tier (
    risk_tier_sk            SERIAL          PRIMARY KEY,
    tier_code               VARCHAR(20)     NOT NULL,
    tier_name               VARCHAR(50)     NOT NULL,
    score_lower_bound       DECIMAL(5,4),
    score_upper_bound       DECIMAL(5,4),
    UNIQUE (tier_code)
);

CREATE TABLE IF NOT EXISTS mart.dim_customer_segment (
    segment_sk              SERIAL          PRIMARY KEY,
    risk_tier_sk            INTEGER         NOT NULL REFERENCES mart.dim_customer_risk_tier(risk_tier_sk),
    segment_code            VARCHAR(20)     NOT NULL,
    segment_name            VARCHAR(100)    NOT NULL,
    UNIQUE (segment_code)
);

CREATE TABLE IF NOT EXISTS mart.dim_customer (
    customer_sk             SERIAL          PRIMARY KEY,
    customer_unique_id      VARCHAR(64)     NOT NULL,
    segment_sk              INTEGER         NOT NULL REFERENCES mart.dim_customer_segment(segment_sk),
    geo_sk                  INTEGER         REFERENCES mart.dim_geo(geo_sk),
    -- SCD2 columns
    valid_from              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    valid_to                TIMESTAMPTZ,
    is_current              BOOLEAN         NOT NULL DEFAULT true,
    dbt_updated_at          TIMESTAMPTZ,
    UNIQUE (customer_unique_id, valid_from)
);

-- ============================================================================
-- SELLER HIERARCHY
-- Chain: dim_seller_industry <- dim_seller_category <- dim_seller
-- SCD2 on dim_seller: new record created when risk tier changes
-- ============================================================================

CREATE TABLE IF NOT EXISTS mart.dim_seller_industry (
    industry_sk             SERIAL          PRIMARY KEY,
    industry_code           VARCHAR(20)     NOT NULL,
    industry_name           VARCHAR(100)    NOT NULL,
    UNIQUE (industry_code)
);

CREATE TABLE IF NOT EXISTS mart.dim_seller_category (
    seller_category_sk      SERIAL          PRIMARY KEY,
    industry_sk             INTEGER         NOT NULL REFERENCES mart.dim_seller_industry(industry_sk),
    category_code           VARCHAR(20)     NOT NULL,
    category_name           VARCHAR(100)    NOT NULL,
    UNIQUE (category_code)
);

CREATE TABLE IF NOT EXISTS mart.dim_seller (
    seller_sk               SERIAL          PRIMARY KEY,
    seller_id               VARCHAR(64)     NOT NULL,
    seller_category_sk      INTEGER         NOT NULL REFERENCES mart.dim_seller_category(seller_category_sk),
    geo_sk                  INTEGER         REFERENCES mart.dim_geo(geo_sk),
    risk_tier               VARCHAR(20)     CHECK (risk_tier IN ('low','medium','high','critical')),
    seller_reliability_score DECIMAL(5,4),
    is_new_seller           BOOLEAN         NOT NULL DEFAULT false,
    -- SCD2 columns
    valid_from              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    valid_to                TIMESTAMPTZ,
    is_current              BOOLEAN         NOT NULL DEFAULT true,
    dbt_updated_at          TIMESTAMPTZ,
    UNIQUE (seller_id, valid_from)
);

-- ============================================================================
-- PRODUCT HIERARCHY
-- Chain: dim_product_department <- dim_product_category <- dim_product
-- No SCD2 - products do not change after listing
-- English category names used throughout (Portuguese only in bronze)
-- ============================================================================

CREATE TABLE IF NOT EXISTS mart.dim_product_department (
    department_sk           SERIAL          PRIMARY KEY,
    department_code         VARCHAR(50)     NOT NULL,
    department_name         VARCHAR(100)    NOT NULL,
    UNIQUE (department_code)
);

CREATE TABLE IF NOT EXISTS mart.dim_product_category (
    product_category_sk     SERIAL          PRIMARY KEY,
    department_sk           INTEGER         NOT NULL REFERENCES mart.dim_product_department(department_sk),
    category_code           VARCHAR(100)    NOT NULL,
    category_name_english   VARCHAR(100)    NOT NULL,
    category_name_portuguese VARCHAR(100),
    UNIQUE (category_code)
);

CREATE TABLE IF NOT EXISTS mart.dim_product (
    product_sk              SERIAL          PRIMARY KEY,
    product_id              VARCHAR(64)     NOT NULL,
    product_category_sk     INTEGER         REFERENCES mart.dim_product_category(product_category_sk),
    product_name_length     INTEGER,
    product_description_length INTEGER,
    product_photos_qty      INTEGER,
    product_weight_g        DECIMAL(10,1),
    product_length_cm       DECIMAL(6,1),
    product_height_cm       DECIMAL(6,1),
    product_width_cm        DECIMAL(6,1),
    UNIQUE (product_id)
);

-- ============================================================================
-- PAYMENT METHOD DIMENSION
-- Reference dimension - no SCD2, no hierarchy
-- Includes not_defined to handle the 3 rows found in exploration
-- ============================================================================

CREATE TABLE IF NOT EXISTS mart.dim_payment_method (
    payment_method_sk       SERIAL          PRIMARY KEY,
    method_code             VARCHAR(30)     NOT NULL,
    method_name             VARCHAR(100),
    method_family           VARCHAR(30)     CHECK (method_family IN (
                                'card', 'bank_slip', 'digital_wallet', 'other'
                            )),
    is_reversible           BOOLEAN         DEFAULT true,
    chargeback_eligible     BOOLEAN         DEFAULT true,
    UNIQUE (method_code)
);

-- ============================================================================
-- TIME DIMENSION
-- Flattened single table - no separate dim_day or dim_month
-- Decision: at Olist scale (100k orders, 2016-2018) three-level hierarchy
-- adds join complexity with no analytical benefit
-- time_sk format: YYYYMMDDHH integer for fast joins
-- ============================================================================

CREATE TABLE IF NOT EXISTS mart.dim_time (
    time_sk                 INTEGER         PRIMARY KEY,
    date_actual             DATE            NOT NULL,
    year_number             SMALLINT        NOT NULL,
    month_number            SMALLINT        NOT NULL,
    month_name              VARCHAR(20),
    quarter_number          SMALLINT        NOT NULL,
    day_of_month            SMALLINT        NOT NULL,
    day_of_week             SMALLINT        NOT NULL,
    day_name                VARCHAR(20),
    hour_of_day             SMALLINT        NOT NULL,
    is_weekend              BOOLEAN         NOT NULL,
    is_business_hours       BOOLEAN         NOT NULL,
    time_band               VARCHAR(20)     CHECK (time_band IN (
                                'early_morning',
                                'morning',
                                'afternoon',
                                'evening',
                                'night'
                            )),
    UNIQUE (date_actual, hour_of_day)
);
