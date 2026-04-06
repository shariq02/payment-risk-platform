-- models/marts/dimensions/dim_seller.sql
-- ============================================================================
-- Dimension: seller
-- Grain: one row per seller_id (current state)
-- Source: int_seller_performance
-- SCD2: handled by snap_seller_risk_profile snapshot
-- ============================================================================

{{
    config(
        materialized='table',
        schema='mart',
        unique_key='seller_id'
    )
}}

WITH seller_perf AS (
    SELECT * FROM {{ ref('int_seller_performance') }}
),

categories AS (
    SELECT seller_category_sk, category_code
    FROM mart.dim_seller_category
    WHERE category_code = 'marketplace_general'
    LIMIT 1
),

-- Use dim_geo from dbt model (has geo_sk from ROW_NUMBER)
geo AS (
    SELECT geo_sk, zip_code_prefix
    FROM {{ ref('dim_geo') }}
),

final AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY sp.seller_id) AS seller_sk,
        sp.seller_id,
        COALESCE(sc.seller_category_sk, 1)      AS seller_category_sk,
        COALESCE(g.geo_sk, -1)                  AS geo_sk,
        sp.risk_tier,
        sp.seller_reliability_score,
        sp.is_new_seller,
        sp.total_orders_30d,
        sp.late_delivery_rate_30d,
        sp.avg_review_score_30d,
        sp.cancellation_rate_30d,
        sp.insufficient_data,

        -- NaN handling for first_order_ts
        CASE WHEN sp.first_order_ts::TEXT = 'NaN'
             THEN NULL
             ELSE sp.first_order_ts
        END                                     AS first_order_ts,

        sp.seller_zip_code_prefix,
        sp.seller_city,
        sp.seller_state,
        NOW()                                   AS valid_from,
        NULL::TIMESTAMPTZ                       AS valid_to,
        true                                    AS is_current,
        NOW()                                   AS dbt_updated_at
    FROM seller_perf sp
    CROSS JOIN categories sc
    LEFT JOIN geo g ON sp.seller_zip_code_prefix = g.zip_code_prefix
)

SELECT * FROM final
