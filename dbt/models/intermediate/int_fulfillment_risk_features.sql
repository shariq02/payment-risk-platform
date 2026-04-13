-- models/intermediate/int_fulfillment_risk_features.sql
-- ============================================================================
-- Intermediate: fulfillment risk features and score computation
-- Materialization: ephemeral (compiled into downstream queries)
-- Dependencies: stg_order_items, stg_orders, stg_reviews,
--               int_seller_performance
-- Output: one row per order item with fulfillment_risk_score
--
-- Used by: fact_order_fulfillment (via marts)
--
-- fulfillment_risk_score signals and weights:
--   is_cancelled:                            +0.35
--   is_late_delivery:                        +0.30
--   review_score <= 2 (dispute proxy):       +0.20
--   seller late_delivery_rate > 15%:         +0.10
--   seller is new (< 30 days):               +0.05
--
-- Score capped at 0.0 to 1.0
-- is_dispute_proxy flag: review_score <= 2
-- ============================================================================

WITH items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

reviews AS (
    SELECT * FROM {{ ref('stg_reviews') }}
),

seller_perf AS (
    SELECT * FROM {{ ref('int_seller_performance') }}
),

-- Join items to orders
base AS (
    SELECT
        i.order_id,
        i.order_item_id,
        i.product_id,
        i.seller_id,
        i.item_price,
        i.freight_value,
        i.shipping_limit_ts,
        o.order_purchase_ts                     AS event_ts,
        o.order_status,
        o.is_late_delivery,
        o.is_cancelled
    FROM items i
    JOIN orders o ON i.order_id = o.order_id
),

-- Join reviews (one per order after deduplication in staging)
with_reviews AS (
    SELECT
        b.*,
        r.review_score,
        COALESCE(r.is_dispute_proxy, false)     AS is_dispute_proxy
    FROM base b
    LEFT JOIN reviews r ON b.order_id = r.order_id
),

-- Join seller performance metrics
with_seller AS (
    SELECT
        wr.*,
        COALESCE(sp.late_delivery_rate_30d, 0)  AS seller_late_delivery_rate,
        COALESCE(sp.is_new_seller, false)        AS seller_is_new,
        COALESCE(sp.risk_tier, 'medium')         AS seller_risk_tier,
        COALESCE(sp.seller_reliability_score, 0.65) AS seller_reliability_score
    FROM with_reviews wr
    LEFT JOIN seller_perf sp ON wr.seller_id = sp.seller_id
),

-- Compute fulfillment risk score
scored AS (
    SELECT
        *,

        -- Composite fulfillment risk score
        -- Weights documented in DESIGN_AMENDMENTS and DECISION_LOG
        GREATEST(0.0, LEAST(1.0,
            CASE WHEN is_cancelled
                 THEN 0.35 ELSE 0.0 END
            + CASE WHEN is_late_delivery
                   THEN 0.30 ELSE 0.0 END
            + CASE WHEN is_dispute_proxy
                   THEN 0.20 ELSE 0.0 END
            + CASE WHEN seller_late_delivery_rate
                   > {{ var('seller_late_delivery_threshold') }}
                   THEN 0.10 ELSE 0.0 END
            + CASE WHEN seller_is_new
                   THEN 0.05 ELSE 0.0 END
        ))                                       AS fulfillment_risk_score

    FROM with_seller
)

SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    item_price,
    freight_value,
    shipping_limit_ts,
    event_ts,
    order_status,
    is_late_delivery,
    is_cancelled,
    review_score,
    is_dispute_proxy,
    seller_late_delivery_rate,
    seller_is_new,
    seller_risk_tier,
    seller_reliability_score,
    fulfillment_risk_score

FROM scored
