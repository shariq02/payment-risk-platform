-- models/marts/facts/fact_order_payments.sql
-- ============================================================================
-- Fact: order payments
-- Grain: one row per payment transaction
-- Business process: payment risk
-- Source: int_payment_risk_features
--
-- Dimensions joined:
--   dim_customer  - SCD2 point-in-time join on customer_unique_id
--   dim_geo       - via customer_zip_code_prefix
--   dim_payment_method - via payment_type
--   dim_time      - via order_purchase_ts hour
--
-- CRITICAL SCD2 JOIN PATTERN:
--   Point-in-time join used for dim_customer - NOT is_current = true
--   is_current join gives today's segment on historical transactions
--   event_ts BETWEEN valid_from AND valid_to gives correct historical segment
--   This distinction is the most common SCD2 interview failure point
--
-- CROSS-FACT RECONCILIATION WARNING:
--   Joining this fact to fact_order_fulfillment on order_id creates cartesian
--   product (multiple payment rows x multiple item rows per order)
--   Always aggregate one side before joining
--   See sql/analytics/order_reconciliation_examples.sql
-- ============================================================================

{{
    config(
        materialized='table',
        schema='mart',
        unique_key=['order_id', 'payment_sequential']
    )
}}

WITH payments AS (
    SELECT * FROM {{ ref('int_payment_risk_features') }}
),

-- SCD2 point-in-time customer lookup
-- Finds the customer record that was active when the payment occurred
dim_customer AS (
    SELECT
        customer_sk,
        customer_unique_id,
        valid_from,
        valid_to
    FROM mart.dim_customer
),

-- Geo lookup via zip code
dim_geo AS (
    SELECT
        geo_sk,
        zip_code_prefix
    FROM mart.dim_geo
),

-- Payment method lookup
dim_payment AS (
    SELECT
        payment_method_sk,
        method_code
    FROM mart.dim_payment_method
),

-- Time dimension lookup
dim_time AS (
    SELECT
        time_sk,
        date_actual,
        hour_of_day
    FROM mart.dim_time
),

final AS (
    SELECT
        -- Natural keys
        p.order_id,
        p.payment_sequential,

        -- Dimension surrogate keys
        -- SCD2 point-in-time join for customer
        COALESCE(c.customer_sk, -1)             AS customer_sk,
        COALESCE(g.geo_sk, -1)                  AS geo_sk,
        COALESCE(pm.payment_method_sk, -1)      AS payment_method_sk,
        COALESCE(t.time_sk, -1)                 AS time_sk,

        -- Measures
        p.payment_value,
        p.payment_installments,

        -- Risk signals
        p.payment_risk_score,
        p.is_dispute_risk,
        p.is_high_value,

        -- Order context
        p.order_status,
        p.customer_unique_id,

        -- Audit
        p.event_ts,
        NOW()                                   AS ingested_at,
        NOW()                                   AS dbt_updated_at

    FROM payments p

    -- SCD2 point-in-time join
    -- Finds customer record active at time of payment
    LEFT JOIN dim_customer c
        ON p.customer_unique_id = c.customer_unique_id
        AND p.event_ts >= c.valid_from
        AND (p.event_ts < c.valid_to OR c.valid_to IS NULL)

    -- Geo join via customer zip
    LEFT JOIN dim_geo g
        ON p.customer_zip_code_prefix = g.zip_code_prefix

    -- Payment method join
    LEFT JOIN dim_payment pm
        ON p.payment_type = pm.method_code

    -- Time join - match on date and hour
    LEFT JOIN dim_time t
        ON t.date_actual = p.event_ts::DATE
        AND t.hour_of_day = EXTRACT(HOUR FROM p.event_ts)::SMALLINT
)

SELECT * FROM final
