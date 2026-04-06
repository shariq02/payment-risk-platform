-- models/marts/facts/fact_order_fulfillment.sql
-- ============================================================================
-- Fact: order fulfillment
-- Grain: one row per order item
-- Business process: fulfillment risk
-- Source: int_fulfillment_risk_features
--
-- Dimensions joined:
--   dim_seller  - SCD2 point-in-time join on seller_id
--   dim_product - via product_id
--   dim_time    - via order_purchase_ts hour
--
-- CRITICAL SCD2 JOIN PATTERN:
--   Point-in-time join used for dim_seller - NOT is_current = true
--   is_current join gives today's risk tier on historical item deliveries
--   event_ts BETWEEN valid_from AND valid_to gives correct historical tier
--
-- CROSS-FACT RECONCILIATION WARNING:
--   Joining to fact_order_payments on order_id creates cartesian product
--   Always aggregate one side before joining
--   See sql/analytics/order_reconciliation_examples.sql
-- ============================================================================

{{
    config(
        materialized='table',
        schema='mart',
        unique_key=['order_id', 'order_item_id']
    )
}}

WITH fulfillment AS (
    SELECT * FROM {{ ref('int_fulfillment_risk_features') }}
),

-- SCD2 point-in-time seller lookup
dim_seller AS (
    SELECT
        seller_sk,
        seller_id,
        valid_from,
        valid_to
    FROM mart.dim_seller
),

-- Product lookup
dim_product AS (
    SELECT
        product_sk,
        product_id
    FROM mart.dim_product
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
        f.order_id,
        f.order_item_id,

        -- Dimension surrogate keys
        -- SCD2 point-in-time join for seller
        COALESCE(s.seller_sk, -1)               AS seller_sk,
        COALESCE(p.product_sk, -1)              AS product_sk,
        COALESCE(t.time_sk, -1)                 AS time_sk,

        -- Measures
        f.item_price,
        f.freight_value,

        -- Risk signals
        f.fulfillment_risk_score,
        f.is_late_delivery,
        f.is_cancelled,
        f.is_dispute_proxy,
        f.review_score,

        -- Seller context
        f.seller_id,

        -- Audit
        f.event_ts,
        NOW()                                   AS ingested_at,
        NOW()                                   AS dbt_updated_at

    FROM fulfillment f

    -- SCD2 point-in-time join for seller
    -- Finds seller record active at time of order item
    LEFT JOIN dim_seller s
        ON f.seller_id = s.seller_id
        AND f.event_ts >= s.valid_from
        AND (f.event_ts < s.valid_to OR s.valid_to IS NULL)

    -- Product join
    LEFT JOIN dim_product p
        ON f.product_id = p.product_id

    -- Time join
    LEFT JOIN dim_time t
        ON t.date_actual = f.event_ts::DATE
        AND t.hour_of_day = EXTRACT(HOUR FROM f.event_ts)::SMALLINT
)

SELECT * FROM final
