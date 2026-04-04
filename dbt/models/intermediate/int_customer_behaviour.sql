-- models/intermediate/int_customer_behaviour.sql
-- ============================================================================
-- Intermediate: customer behaviour and segment assignment
-- Materialization: ephemeral (compiled into downstream queries)
-- Dependencies: stg_customers, stg_orders, stg_order_payments, stg_reviews
-- Output: one row per customer_unique_id with behaviour features
--
-- Used by: int_payment_risk_features, dim_customer (via marts)
--
-- IMPORTANT: Uses customer_unique_id as the real customer identifier.
-- customer_id is per-order. One real customer can have multiple customer_ids
-- across different orders. Aggregations done at customer_unique_id level.
--
-- Segments assigned:
--   new_customer:       fewer than 2 orders
--   returning_customer: 2+ orders, avg payment below 300 BRL
--   high_value_customer: avg payment above 300 BRL
--   at_risk_customer:   has any prior dispute proxy (review_score <= 2)
--
-- Segment priority: at_risk overrides others if customer has dispute history
-- ============================================================================

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

payments AS (
    SELECT * FROM {{ ref('stg_order_payments') }}
),

reviews AS (
    SELECT * FROM {{ ref('stg_reviews') }}
),

-- Join customers to orders via customer_id
-- Then aggregate at customer_unique_id level
customer_orders AS (
    SELECT
        c.customer_unique_id,
        c.customer_zip_code_prefix,
        c.customer_city,
        c.customer_state,
        o.order_id,
        o.order_purchase_ts,
        o.order_status,
        o.is_cancelled
    FROM customers c
    JOIN orders o ON c.customer_id = o.customer_id
),

-- Join payments to get payment values per order
customer_payments AS (
    SELECT
        co.customer_unique_id,
        co.order_id,
        co.order_purchase_ts,
        co.is_cancelled,
        SUM(p.payment_value)        AS order_payment_value,
        MAX(p.payment_type)         AS payment_type,
        MAX(p.payment_installments) AS max_installments
    FROM customer_orders co
    JOIN payments p ON co.order_id = p.order_id
    GROUP BY
        co.customer_unique_id,
        co.order_id,
        co.order_purchase_ts,
        co.is_cancelled
),

-- Join reviews to check dispute history
customer_reviews AS (
    SELECT
        co.customer_unique_id,
        co.order_id,
        r.review_score,
        r.is_dispute_proxy
    FROM customer_orders co
    LEFT JOIN reviews r ON co.order_id = r.order_id
),

-- Aggregate all metrics at customer_unique_id level
customer_metrics AS (
    SELECT
        cp.customer_unique_id,
        COUNT(DISTINCT cp.order_id)             AS total_orders,
        SUM(cp.order_payment_value)             AS total_payment_value,
        AVG(cp.order_payment_value)             AS avg_payment_value,
        MIN(cp.order_purchase_ts)               AS first_order_ts,
        MAX(cp.order_purchase_ts)               AS last_order_ts,

        -- Most frequent payment type
        MODE() WITHIN GROUP (
            ORDER BY cp.payment_type
        )                                       AS preferred_payment_type,

        -- Average installments
        AVG(cp.max_installments)                AS avg_installments,

        -- Dispute history
        MAX(CASE WHEN cr.is_dispute_proxy
            THEN 1 ELSE 0 END)                  AS has_dispute_history,

        -- Cancellation count
        COUNT(DISTINCT CASE WHEN cp.is_cancelled
            THEN cp.order_id END)               AS cancelled_orders

    FROM customer_payments cp
    LEFT JOIN customer_reviews cr
        ON cp.customer_unique_id = cr.customer_unique_id
        AND cp.order_id = cr.order_id
    GROUP BY cp.customer_unique_id
),

-- Join back to get zip code for geo lookup
customer_geo AS (
    SELECT DISTINCT ON (customer_unique_id)
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    FROM customer_orders
    ORDER BY customer_unique_id, order_purchase_ts DESC
),

-- Combine metrics with geo
combined AS (
    SELECT
        m.*,
        g.customer_zip_code_prefix,
        g.customer_city,
        g.customer_state
    FROM customer_metrics m
    LEFT JOIN customer_geo g ON m.customer_unique_id = g.customer_unique_id
),

-- Assign customer segment
-- Priority: at_risk overrides all others
segmented AS (
    SELECT
        *,
        CASE
            WHEN has_dispute_history = 1
                THEN 'at_risk_customer'
            WHEN avg_payment_value > {{ var('high_value_threshold') }}
                THEN 'high_value_customer'
            WHEN total_orders >= 2
                THEN 'returning_customer'
            ELSE 'new_customer'
        END                                     AS segment_code,

        -- Risk tier based on dispute history and order behaviour
        CASE
            WHEN has_dispute_history = 1 THEN 'tier_3'
            WHEN avg_payment_value > {{ var('high_value_threshold') }} THEN 'tier_1'
            WHEN total_orders >= 2 THEN 'tier_1'
            ELSE 'tier_2'
        END                                     AS risk_tier_code

    FROM combined
)

SELECT * FROM segmented
