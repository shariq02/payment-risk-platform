-- models/staging/stg_order_items.sql
-- ============================================================================
-- Staging: order items
-- Source: bronze.order_items
-- Grain: one row per item within an order (112,650 rows)
-- Changes from bronze:
--   - price and freight_value cast from VARCHAR to DECIMAL
--   - shipping_limit_date cast to TIMESTAMPTZ
--   - No filtering - all items kept
-- ============================================================================

WITH source AS (
    SELECT * FROM {{ source('bronze', 'order_items') }}
),

typed AS (
    SELECT
        order_id,
        CAST(order_item_id   AS SMALLINT)      AS order_item_id,
        product_id,
        seller_id,
        CAST(shipping_limit_date AS TIMESTAMPTZ) AS shipping_limit_ts,
        CAST(price           AS DECIMAL(10,2))  AS item_price,
        CAST(freight_value   AS DECIMAL(10,2))  AS freight_value,
        _ingested_at,
        _source_file
    FROM source
    WHERE order_id IS NOT NULL
      AND order_item_id IS NOT NULL
)

SELECT * FROM typed
