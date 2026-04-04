-- models/intermediate/int_payment_risk_features.sql
-- ============================================================================
-- Intermediate: payment risk features and score computation
-- Materialization: ephemeral (compiled into downstream queries)
-- Dependencies: stg_order_payments, stg_orders, stg_customers,
--               int_customer_behaviour
-- Output: one row per payment transaction with payment_risk_score
--
-- Used by: fact_order_payments (via marts)
--
-- payment_risk_score signals and weights:
--   payment_type = voucher or not_defined:  +0.25
--   payment_installments > 6:               +0.20
--   customer has prior dispute proxy:        +0.30
--   high value payment (above threshold):    +0.15
--   customer is new (first order):           +0.10
--
-- Score capped at 0.0 to 1.0
-- is_dispute_risk flag: score >= high_risk_score_threshold (default 0.7)
-- is_high_value flag: payment_value >= high_value_threshold (default 500)
-- ============================================================================

WITH payments AS (
    SELECT * FROM {{ ref('stg_order_payments') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

customer_behaviour AS (
    SELECT * FROM {{ ref('int_customer_behaviour') }}
),

-- Join payments to orders and customers
base AS (
    SELECT
        p.order_id,
        p.payment_sequential,
        p.payment_type,
        p.payment_installments,
        p.payment_value,
        o.order_purchase_ts                     AS event_ts,
        o.order_status,
        o.is_cancelled,
        c.customer_unique_id,
        c.customer_zip_code_prefix
    FROM payments p
    JOIN orders o ON p.order_id = o.order_id
    JOIN customers c ON o.customer_id = c.customer_id
),

-- Join customer behaviour features
enriched AS (
    SELECT
        b.*,
        COALESCE(cb.has_dispute_history, 0)     AS has_dispute_history,
        COALESCE(cb.total_orders, 1)            AS customer_total_orders,
        COALESCE(cb.segment_code, 'new_customer') AS segment_code,
        COALESCE(cb.risk_tier_code, 'tier_2')   AS risk_tier_code
    FROM base b
    LEFT JOIN customer_behaviour cb
        ON b.customer_unique_id = cb.customer_unique_id
),

-- Compute payment risk score
scored AS (
    SELECT
        *,

        -- Individual risk signal flags
        CASE WHEN payment_type IN ('voucher', 'not_defined')
             THEN true ELSE false END            AS is_risky_payment_type,

        CASE WHEN payment_installments > 6
             THEN true ELSE false END            AS is_high_installments,

        CASE WHEN has_dispute_history = 1
             THEN true ELSE false END            AS has_prior_dispute,

        CASE WHEN payment_value >= {{ var('high_value_threshold') }}
             THEN true ELSE false END            AS is_high_value,

        CASE WHEN customer_total_orders = 1
             THEN true ELSE false END            AS is_new_customer,

        -- Composite payment risk score
        -- Weights documented in DESIGN_AMENDMENTS and DECISION_LOG
        GREATEST(0.0, LEAST(1.0,
            CASE WHEN payment_type IN ('voucher', 'not_defined')
                 THEN 0.25 ELSE 0.0 END
            + CASE WHEN payment_installments > 6
                   THEN 0.20 ELSE 0.0 END
            + CASE WHEN has_dispute_history = 1
                   THEN 0.30 ELSE 0.0 END
            + CASE WHEN payment_value >= {{ var('high_value_threshold') }}
                   THEN 0.15 ELSE 0.0 END
            + CASE WHEN customer_total_orders = 1
                   THEN 0.10 ELSE 0.0 END
        ))                                       AS payment_risk_score

    FROM enriched
),

-- Add derived flags based on score
final AS (
    SELECT
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value,
        event_ts,
        order_status,
        is_cancelled,
        customer_unique_id,
        customer_zip_code_prefix,
        has_dispute_history,
        customer_total_orders,
        segment_code,
        risk_tier_code,
        is_risky_payment_type,
        is_high_installments,
        has_prior_dispute,
        is_high_value,
        is_new_customer,
        payment_risk_score,

        -- Dispute risk flag: score above threshold
        CASE WHEN payment_risk_score >= {{ var('high_risk_score_threshold') }}
             THEN true ELSE false END            AS is_dispute_risk

    FROM scored
)

SELECT * FROM final
