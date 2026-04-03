-- ============================================================================
-- Indexes DDL
-- Payment Risk and Order Analytics Platform
-- ============================================================================
-- File: sql/ddl/05_create_indexes.sql
-- Purpose: Creates all indexes on mart dimension and fact tables.
--          Indexes created after tables and data are loaded for performance.
--          Partial indexes used where appropriate to reduce index size.
-- Run: Once during Phase 3 setup via ingestion/setup_warehouse.py
-- Safe to re-run: yes (uses IF NOT EXISTS)
-- ============================================================================

-- ============================================================================
-- FACT_ORDER_PAYMENTS INDEXES
-- Most common analytical access patterns:
--   - Time range queries (fraud monitoring last N hours/days)
--   - Customer history queries (risk profile per customer)
--   - High risk alert queries (risk score above threshold)
--   - Order lookup (debugging and reconciliation)
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_payments_event_ts
    ON mart.fact_order_payments (event_ts DESC);

CREATE INDEX IF NOT EXISTS idx_payments_customer_sk
    ON mart.fact_order_payments (customer_sk, event_ts DESC);

CREATE INDEX IF NOT EXISTS idx_payments_payment_method_sk
    ON mart.fact_order_payments (payment_method_sk);

CREATE INDEX IF NOT EXISTS idx_payments_time_sk
    ON mart.fact_order_payments (time_sk);

-- Partial index - only high risk payments
-- Smaller index, faster for alert queries
CREATE INDEX IF NOT EXISTS idx_payments_risk_score
    ON mart.fact_order_payments (payment_risk_score DESC)
    WHERE payment_risk_score > 0.5;

-- Partial index - dispute risk flag
CREATE INDEX IF NOT EXISTS idx_payments_dispute_risk
    ON mart.fact_order_payments (event_ts DESC)
    WHERE is_dispute_risk = true;

-- Natural key lookup for debugging
CREATE INDEX IF NOT EXISTS idx_payments_order_id
    ON mart.fact_order_payments (order_id);

-- Customer natural key lookup (for API endpoint)
CREATE INDEX IF NOT EXISTS idx_payments_customer_unique_id
    ON mart.fact_order_payments (customer_unique_id, event_ts DESC);

-- ============================================================================
-- FACT_ORDER_FULFILLMENT INDEXES
-- Most common analytical access patterns:
--   - Seller performance queries
--   - Late delivery monitoring
--   - Product category risk analysis
--   - Cross-fact reconciliation via order_id
-- ============================================================================

CREATE INDEX IF NOT EXISTS idx_fulfillment_event_ts
    ON mart.fact_order_fulfillment (event_ts DESC);

CREATE INDEX IF NOT EXISTS idx_fulfillment_seller_sk
    ON mart.fact_order_fulfillment (seller_sk, event_ts DESC);

CREATE INDEX IF NOT EXISTS idx_fulfillment_product_sk
    ON mart.fact_order_fulfillment (product_sk);

CREATE INDEX IF NOT EXISTS idx_fulfillment_time_sk
    ON mart.fact_order_fulfillment (time_sk);

-- Partial index - only high risk fulfillments
CREATE INDEX IF NOT EXISTS idx_fulfillment_risk_score
    ON mart.fact_order_fulfillment (fulfillment_risk_score DESC)
    WHERE fulfillment_risk_score > 0.5;

-- Partial index - late deliveries only
CREATE INDEX IF NOT EXISTS idx_fulfillment_late_delivery
    ON mart.fact_order_fulfillment (event_ts DESC)
    WHERE is_late_delivery = true;

-- Cross-fact join index - order_id used to join payments to fulfillment
CREATE INDEX IF NOT EXISTS idx_fulfillment_order_id
    ON mart.fact_order_fulfillment (order_id);

-- Seller natural key lookup (for API endpoint)
CREATE INDEX IF NOT EXISTS idx_fulfillment_seller_id
    ON mart.fact_order_fulfillment (seller_id, event_ts DESC);

-- ============================================================================
-- DIMENSION INDEXES
-- SCD2 current-record lookups are the most common dimension access pattern
-- Partial indexes on is_current = true keep them small and fast
-- ============================================================================

-- dim_customer - SCD2 current record lookup
CREATE INDEX IF NOT EXISTS idx_customer_current
    ON mart.dim_customer (customer_unique_id)
    WHERE is_current = true;

-- dim_customer - point-in-time join support
CREATE INDEX IF NOT EXISTS idx_customer_valid_from
    ON mart.dim_customer (customer_unique_id, valid_from, valid_to);

-- dim_seller - SCD2 current record lookup
CREATE INDEX IF NOT EXISTS idx_seller_current
    ON mart.dim_seller (seller_id)
    WHERE is_current = true;

-- dim_seller - point-in-time join support
CREATE INDEX IF NOT EXISTS idx_seller_valid_from
    ON mart.dim_seller (seller_id, valid_from, valid_to);

-- dim_geo - zip code lookup (used to join customers and sellers)
CREATE INDEX IF NOT EXISTS idx_geo_zip_code
    ON mart.dim_geo (zip_code_prefix);

-- dim_product - product lookup from order items
CREATE INDEX IF NOT EXISTS idx_product_id
    ON mart.dim_product (product_id);

-- dim_time - date lookup for time dimension population
CREATE INDEX IF NOT EXISTS idx_time_date_actual
    ON mart.dim_time (date_actual);

-- risk_kpis_daily - date range queries for dashboard
CREATE INDEX IF NOT EXISTS idx_kpis_date_actual
    ON mart.risk_kpis_daily (date_actual DESC);
