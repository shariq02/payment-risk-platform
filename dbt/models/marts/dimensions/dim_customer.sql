-- models/marts/dimensions/dim_customer.sql
-- ============================================================================
-- Dimension: customer
-- Grain: one row per customer_unique_id (current state)
-- Source: int_customer_behaviour
-- SCD2: handled by snap_customer_profile snapshot
-- ============================================================================

{{
    config(
        materialized='table',
        schema='mart',
        unique_key='customer_unique_id'
    )
}}

WITH customer_behaviour AS (
    SELECT * FROM {{ ref('int_customer_behaviour') }}
),

segments AS (
    SELECT segment_sk, segment_code
    FROM mart.dim_customer_segment
),

-- Use dim_geo from dbt model (has geo_sk from ROW_NUMBER)
geo AS (
    SELECT geo_sk, zip_code_prefix
    FROM {{ ref('dim_geo') }}
),

final AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY cb.customer_unique_id) AS customer_sk,
        cb.customer_unique_id,
        COALESCE(s.segment_sk, 1)               AS segment_sk,
        COALESCE(g.geo_sk, -1)                  AS geo_sk,
        cb.segment_code,
        cb.risk_tier_code,
        cb.total_orders,
        cb.total_payment_value,
        cb.avg_payment_value,
        cb.preferred_payment_type,
        cb.avg_installments,
        CAST(cb.has_dispute_history AS BOOLEAN)  AS has_dispute_history,
        cb.cancelled_orders,
        cb.first_order_ts,
        cb.last_order_ts,
        cb.customer_zip_code_prefix,
        cb.customer_city,
        cb.customer_state,
        NOW()                                   AS valid_from,
        NULL::TIMESTAMPTZ                       AS valid_to,
        true                                    AS is_current,
        NOW()                                   AS dbt_updated_at
    FROM customer_behaviour cb
    LEFT JOIN segments s ON cb.segment_code = s.segment_code
    LEFT JOIN geo g ON cb.customer_zip_code_prefix = g.zip_code_prefix
)

SELECT * FROM final
