-- models/staging/stg_orders.sql
-- ============================================================================
-- Staging: orders
-- Source: bronze.orders
-- Grain: one row per order (99,441 rows)
-- Changes from bronze:
--   - All timestamp VARCHAR columns cast to TIMESTAMPTZ
--   - Columns renamed for clarity (suffix _ts for timestamps)
--   - No filtering - all order statuses kept including cancelled
--   - is_cancelled and is_delivered flags added for downstream use
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
        -- NULL values preserved as-is (approved_at null for 160 orders)
        CAST(order_purchase_timestamp      AS TIMESTAMPTZ) AS order_purchase_ts,
        CAST(order_approved_at             AS TIMESTAMPTZ) AS order_approved_ts,
        CAST(order_delivered_carrier_date  AS TIMESTAMPTZ) AS order_delivered_carrier_ts,
        CAST(order_delivered_customer_date AS TIMESTAMPTZ) AS order_delivered_customer_ts,
        CAST(order_estimated_delivery_date AS TIMESTAMPTZ) AS order_estimated_delivery_ts,

        -- Convenience flags for downstream models
        CASE WHEN order_status = 'canceled'
             THEN true ELSE false END                       AS is_cancelled,

        CASE WHEN order_status = 'delivered'
             THEN true ELSE false END                       AS is_delivered,

        -- Late delivery flag
        -- True when actual delivery is after estimated delivery
        -- Only meaningful for delivered orders
        CASE
            WHEN order_status = 'delivered'
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
