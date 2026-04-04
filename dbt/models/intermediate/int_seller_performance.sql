-- models/intermediate/int_seller_performance.sql
-- ============================================================================
-- Intermediate: seller performance metrics
-- Materialization: ephemeral (compiled into downstream queries)
-- Dependencies: stg_orders, stg_order_items, stg_reviews, stg_sellers
-- Output: one row per seller with 30-day rolling performance metrics
--
-- Used by: int_fulfillment_risk_features, dim_seller (via marts)
--
-- Metrics computed:
--   total_orders_30d          - order volume in last 30 days
--   late_delivery_rate_30d    - % orders delivered late
--   avg_review_score_30d      - average review score
--   cancellation_rate_30d     - % orders cancelled
--   seller_reliability_score  - weighted composite (0-1, higher = more reliable)
--   is_new_seller             - active less than 30 days
--   first_order_ts            - proxy for seller onboarding date
--
-- Risk tier thresholds (from dbt variables):
--   low:      reliability_score >= 0.85
--   medium:   reliability_score >= 0.65
--   high:     reliability_score >= 0.40
--   critical: reliability_score < 0.40
--
-- Sellers with fewer than 5 orders in 30-day window:
--   flagged as insufficient_data = true
--   assigned medium risk tier by default
-- ============================================================================

WITH sellers AS (
    SELECT * FROM {{ ref('stg_sellers') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

reviews AS (
    SELECT * FROM {{ ref('stg_reviews') }}
),

-- Link orders to sellers via order_items
-- One order can have multiple sellers - each seller-order combination tracked
seller_orders AS (
    SELECT
        i.seller_id,
        o.order_id,
        o.order_status,
        o.order_purchase_ts,
        o.is_late_delivery,
        o.is_cancelled
    FROM items i
    JOIN orders o ON i.order_id = o.order_id
),

-- First order date per seller (proxy for onboarding date)
-- Decision documented in DECISION_LOG.txt
seller_first_order AS (
    SELECT
        seller_id,
        MIN(order_purchase_ts) AS first_order_ts
    FROM seller_orders
    GROUP BY seller_id
),

-- 30-day rolling metrics per seller
seller_metrics_30d AS (
    SELECT
        so.seller_id,
        COUNT(DISTINCT so.order_id)                             AS total_orders_30d,
        COUNT(DISTINCT CASE WHEN so.is_late_delivery
              THEN so.order_id END)                             AS late_orders_30d,
        COUNT(DISTINCT CASE WHEN so.is_cancelled
              THEN so.order_id END)                             AS cancelled_orders_30d,
        AVG(r.review_score)                                     AS avg_review_score_30d
    FROM seller_orders so
    LEFT JOIN reviews r ON so.order_id = r.order_id
    WHERE so.order_purchase_ts >= NOW() - INTERVAL '30 days'
    GROUP BY so.seller_id
),

-- Combine all metrics
combined AS (
    SELECT
        s.seller_id,
        s.seller_zip_code_prefix,
        s.seller_city,
        s.seller_state,
        fo.first_order_ts,

        -- Is new seller: first order within last 30 days
        CASE WHEN fo.first_order_ts >= NOW() - INTERVAL '30 days'
             THEN true ELSE false END                           AS is_new_seller,

        -- 30-day metrics (default to 0 if no recent orders)
        COALESCE(m.total_orders_30d, 0)                        AS total_orders_30d,
        COALESCE(m.late_orders_30d, 0)                         AS late_orders_30d,
        COALESCE(m.cancelled_orders_30d, 0)                    AS cancelled_orders_30d,
        COALESCE(m.avg_review_score_30d, 3.0)                  AS avg_review_score_30d,

        -- Insufficient data flag: fewer than 5 orders in window
        CASE WHEN COALESCE(m.total_orders_30d, 0) < {{ var('seller_min_orders_threshold') }}
             THEN true ELSE false END                           AS insufficient_data,

        -- Rate calculations (avoid division by zero)
        CASE WHEN COALESCE(m.total_orders_30d, 0) > 0
             THEN COALESCE(m.late_orders_30d, 0)::DECIMAL
                  / m.total_orders_30d
             ELSE 0 END                                         AS late_delivery_rate_30d,

        CASE WHEN COALESCE(m.total_orders_30d, 0) > 0
             THEN COALESCE(m.cancelled_orders_30d, 0)::DECIMAL
                  / m.total_orders_30d
             ELSE 0 END                                         AS cancellation_rate_30d

    FROM sellers s
    LEFT JOIN seller_first_order fo ON s.seller_id = fo.seller_id
    LEFT JOIN seller_metrics_30d m ON s.seller_id = m.seller_id
),

-- Compute reliability score and risk tier
scored AS (
    SELECT
        *,

        -- Reliability score: inverse of risk signals (higher = more reliable)
        -- Components:
        --   Delivery performance: 40% weight
        --   Review score (normalised to 0-1): 40% weight
        --   Cancellation rate: 20% weight
        GREATEST(0.0, LEAST(1.0,
            (1.0 - late_delivery_rate_30d) * 0.40
            + ((avg_review_score_30d - 1.0) / 4.0) * 0.40
            + (1.0 - cancellation_rate_30d) * 0.20
        ))                                                      AS seller_reliability_score

    FROM combined
),

-- Assign risk tier based on reliability score
tiered AS (
    SELECT
        *,
        CASE
            WHEN insufficient_data THEN 'medium'
            WHEN seller_reliability_score >= 0.85 THEN 'low'
            WHEN seller_reliability_score >= 0.65 THEN 'medium'
            WHEN seller_reliability_score >= 0.40 THEN 'high'
            ELSE 'critical'
        END                                                     AS risk_tier

    FROM scored
)

SELECT * FROM tiered
