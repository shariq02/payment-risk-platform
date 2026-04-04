-- models/staging/stg_order_payments.sql
-- ============================================================================
-- Staging: order payments
-- Source: bronze.order_payments
-- Grain: one row per payment transaction (103,886 rows)
-- Changes from bronze:
--   - payment_value cast from VARCHAR to DECIMAL
--   - payment_installments and payment_sequential cast to SMALLINT
--   - not_defined payment_type kept as-is (3 rows, handled in dim seed)
-- ============================================================================

WITH source AS (
    SELECT * FROM {{ source('bronze', 'order_payments') }}
),

typed AS (
    SELECT
        order_id,
        CAST(payment_sequential    AS SMALLINT)     AS payment_sequential,
        payment_type,
        CAST(payment_installments  AS SMALLINT)     AS payment_installments,
        CAST(payment_value         AS DECIMAL(10,2)) AS payment_value,
        _ingested_at,
        _source_file
    FROM source
    WHERE order_id IS NOT NULL
      AND payment_sequential IS NOT NULL
      AND payment_value IS NOT NULL
)

SELECT * FROM typed
