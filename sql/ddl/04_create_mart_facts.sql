-- ============================================================================
-- Mart Facts DDL
-- Payment Risk and Order Analytics Platform
-- ============================================================================
-- File: sql/ddl/04_create_mart_facts.sql
-- Purpose: Creates both fact tables and the daily KPI aggregate table.
--          Two facts following Kimball one-fact-per-business-process.
--          fact_order_payments: payment risk business process
--          fact_order_fulfillment: fulfillment risk business process
-- Run: Once during Phase 3 setup via ingestion/setup_warehouse.py
-- Safe to re-run: yes (uses IF NOT EXISTS)
-- ============================================================================

-- ============================================================================
-- FACT 1: FACT_ORDER_PAYMENTS
-- Grain: one row per payment transaction
-- Business process: payment risk
-- Dimensions: customer, geo, payment_method, time
-- Risk score: payment_risk_score (fraud, chargeback, payment method signals)
--
-- WHY THIS GRAIN:
-- One order can have multiple payment rows (installments or split methods).
-- Payment risk signals belong at the payment transaction level not order level.
-- Combining seller_sk here would corrupt payment_value aggregates since
-- one order can have multiple sellers requiring row duplication.
--
-- SCD2 JOIN PATTERN (point-in-time correct):
-- JOIN mart.dim_customer c
--   ON f.customer_unique_id = c.customer_unique_id
--   AND f.event_ts >= c.valid_from
--   AND (f.event_ts < c.valid_to OR c.valid_to IS NULL)
-- NOT: JOIN dim_customer ON is_current = true
-- The is_current join gives today's segment on historical transactions.
-- ============================================================================

CREATE TABLE IF NOT EXISTS mart.fact_order_payments (
    -- Surrogate key
    payment_sk                  BIGSERIAL       PRIMARY KEY,

    -- Natural keys (for debugging and idempotent loads - never used for joins)
    order_id                    VARCHAR(64)     NOT NULL,
    payment_sequential          SMALLINT        NOT NULL,

    -- Dimension surrogate keys
    customer_sk                 INTEGER         REFERENCES mart.dim_customer(customer_sk),
    geo_sk                      INTEGER         REFERENCES mart.dim_geo(geo_sk),
    payment_method_sk           INTEGER         REFERENCES mart.dim_payment_method(payment_method_sk),
    time_sk                     INTEGER         REFERENCES mart.dim_time(time_sk),

    -- Measures
    payment_value               DECIMAL(10,2)   NOT NULL,
    payment_installments        SMALLINT        NOT NULL,

    -- Risk signals
    payment_risk_score          DECIMAL(6,5),
    is_dispute_risk             BOOLEAN         NOT NULL DEFAULT false,
    is_high_value               BOOLEAN         NOT NULL DEFAULT false,

    -- Order context (denormalized for convenience - acceptable on fact)
    order_status                VARCHAR(32),
    customer_unique_id          VARCHAR(64),

    -- Audit
    event_ts                    TIMESTAMPTZ,
    ingested_at                 TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    dbt_updated_at              TIMESTAMPTZ,

    UNIQUE (order_id, payment_sequential)
);

-- ============================================================================
-- FACT 2: FACT_ORDER_FULFILLMENT
-- Grain: one row per order item
-- Business process: fulfillment risk
-- Dimensions: seller, product, time
-- Risk score: fulfillment_risk_score (delivery, seller, review signals)
--
-- WHY THIS GRAIN:
-- One order can have multiple items from multiple sellers.
-- Seller and product attribution belongs at the item level.
-- Combining payment_value here would require joining to payments table
-- and splitting value across items which invents numbers not in source data.
--
-- CROSS-FACT RECONCILIATION (order-level combined reporting):
-- SELECT
--   p.order_id,
--   SUM(p.payment_value) AS total_payment_value,
--   SUM(f.item_price)    AS total_item_value
-- FROM mart.fact_order_payments p
-- JOIN mart.fact_order_fulfillment f ON p.order_id = f.order_id
-- GROUP BY p.order_id
-- WARNING: Direct join creates cartesian product (multiple payment rows
-- x multiple item rows per order). Always aggregate one side first.
-- ============================================================================

CREATE TABLE IF NOT EXISTS mart.fact_order_fulfillment (
    -- Surrogate key
    fulfillment_sk              BIGSERIAL       PRIMARY KEY,

    -- Natural keys (for debugging and cross-fact joins via order_id)
    order_id                    VARCHAR(64)     NOT NULL,
    order_item_id               SMALLINT        NOT NULL,

    -- Dimension surrogate keys
    seller_sk                   INTEGER         REFERENCES mart.dim_seller(seller_sk),
    product_sk                  INTEGER         REFERENCES mart.dim_product(product_sk),
    time_sk                     INTEGER         REFERENCES mart.dim_time(time_sk),

    -- Measures
    item_price                  DECIMAL(10,2)   NOT NULL,
    freight_value               DECIMAL(10,2)   NOT NULL,

    -- Risk signals
    fulfillment_risk_score      DECIMAL(6,5),
    is_late_delivery            BOOLEAN         NOT NULL DEFAULT false,
    is_cancelled                BOOLEAN         NOT NULL DEFAULT false,
    is_dispute_proxy            BOOLEAN         NOT NULL DEFAULT false,
    review_score                SMALLINT,

    -- Seller context (denormalized for convenience)
    seller_id                   VARCHAR(64),

    -- Audit
    event_ts                    TIMESTAMPTZ,
    ingested_at                 TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    dbt_updated_at              TIMESTAMPTZ,

    UNIQUE (order_id, order_item_id)
);

-- ============================================================================
-- AGGREGATE: RISK_KPIS_DAILY
-- Pre-aggregated daily KPIs from both fact tables
-- Refreshed every 30 minutes by Airflow DAG 2
--
-- WHY PRE-AGGREGATE:
-- Dashboard and API queries on pre-aggregated daily rows are orders of
-- magnitude faster than scanning both fact tables on every request.
-- At 100k orders this is not critical but demonstrates production thinking.
-- In production both facts could have billions of rows.
-- ============================================================================

CREATE TABLE IF NOT EXISTS mart.risk_kpis_daily (
    kpi_sk                          BIGSERIAL       PRIMARY KEY,
    date_actual                     DATE            NOT NULL,

    -- Payment KPIs (from fact_order_payments)
    total_payment_transactions      INTEGER,
    total_payment_value             DECIMAL(15,2),
    avg_payment_value               DECIMAL(10,2),
    dispute_risk_payment_count      INTEGER,
    dispute_risk_payment_rate       DECIMAL(8,6),
    avg_payment_risk_score          DECIMAL(6,5),
    high_value_payment_count        INTEGER,
    credit_card_count               INTEGER,
    boleto_count                    INTEGER,
    voucher_count                   INTEGER,
    debit_card_count                INTEGER,

    -- Fulfillment KPIs (from fact_order_fulfillment)
    total_order_items               INTEGER,
    total_item_value                DECIMAL(15,2),
    late_delivery_count             INTEGER,
    late_delivery_rate              DECIMAL(8,6),
    cancellation_count              INTEGER,
    cancellation_rate               DECIMAL(8,6),
    avg_fulfillment_risk_score      DECIMAL(6,5),
    dispute_proxy_count             INTEGER,
    avg_review_score                DECIMAL(4,2),
    high_risk_seller_count          INTEGER,
    new_seller_count                INTEGER,

    computed_at                     TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    UNIQUE (date_actual)
);
