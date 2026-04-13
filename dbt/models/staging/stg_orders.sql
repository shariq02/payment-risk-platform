-- models/staging/stg_orders.sql
-- ============================================================================
-- Staging: orders
-- Source: bronze.orders
-- Grain: one row per order (99,441 rows)
-- Changes from bronze:
--   - All timestamp VARCHAR columns cast to TIMESTAMPTZ
--   - NaN string values converted to NULL before casting
--     order_approved_at: 160 NaN rows
--     order_delivered_carrier_date: 1,783 NaN rows
--     order_delivered_customer_date: 2,965 NaN rows
--   - Columns renamed for clarity (suffix _ts for timestamps)
--   - No filtering - all order statuses kept including cancelled
--   - is_cancelled, is_delivered, is_late_delivery flags added
-- ============================================================================

WITH source AS (
    SELECT * FROM {{ source('bronze', 'orders') }}
),

typed AS (
    SELECT
        order_id,
        customer_id,
        order_status,

        -- Cast timestamps from VARCHAR to TIMESTAMPTZ
        -- NaN string values converted to NULL before casting
        CASE WHEN order_purchase_timestamp::TEXT IN ('NaN', '', 'nan')
            THEN NULL
            ELSE CAST(order_purchase_timestamp AS TIMESTAMPTZ)
        END AS order_purchase_ts,

        CASE WHEN order_approved_at::TEXT IN ('NaN', '', 'nan')
            THEN NULL
            ELSE CAST(order_approved_at AS TIMESTAMPTZ)
        END AS order_approved_ts,

        CASE WHEN order_delivered_carrier_date::TEXT IN ('NaN', '', 'nan')
            THEN NULL
            ELSE CAST(order_delivered_carrier_date AS TIMESTAMPTZ)
        END AS order_delivered_carrier_ts,

        CASE WHEN order_delivered_customer_date::TEXT IN ('NaN', '', 'nan')
            THEN NULL
            ELSE CAST(order_delivered_customer_date AS TIMESTAMPTZ)
        END AS order_delivered_customer_ts,

        CASE WHEN order_estimated_delivery_date::TEXT IN ('NaN', '', 'nan')
            THEN NULL
            ELSE CAST(order_estimated_delivery_date AS TIMESTAMPTZ)
        END AS order_estimated_delivery_ts,

        -- Convenience flags
        CASE WHEN order_status = 'canceled'
             THEN true ELSE false END                       AS is_cancelled,

        CASE WHEN order_status = 'delivered'
             THEN true ELSE false END                       AS is_delivered,

        -- Late delivery flag
        -- NaN check added to raw columns before casting
        -- Prevents invalid input syntax error on NaN timestamp values
        CASE
            WHEN order_status = 'delivered'
             AND order_delivered_customer_date::TEXT NOT IN ('NaN', '', 'nan')
             AND order_estimated_delivery_date::TEXT NOT IN ('NaN', '', 'nan')
             AND order_delivered_customer_date IS NOT NULL
             AND order_estimated_delivery_date IS NOT NULL
             AND CAST(order_delivered_customer_date AS TIMESTAMPTZ)
               > CAST(order_estimated_delivery_date AS TIMESTAMPTZ)
            THEN true
            ELSE false
        END                                                 AS is_late_delivery,

        _ingested_at,
        _source_file

    FROM source
    WHERE order_id IS NOT NULL
)

SELECT * FROM typed
