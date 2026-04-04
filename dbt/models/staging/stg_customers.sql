-- models/staging/stg_customers.sql
-- ============================================================================
-- Staging: customers
-- Source: bronze.customers
-- Grain: one row per customer_id (99,441 rows)
-- Changes from bronze:
--   - customer_zip_code_prefix cast to VARCHAR (already VARCHAR, kept clean)
--   - city and state trimmed and lowercased for consistency
--
-- IMPORTANT: customer_id is per-order (99,441 unique)
--            customer_unique_id is the real person identifier (96,096 unique)
--            dim_customer uses customer_unique_id as natural key
--            This staging model keeps both for traceability
-- ============================================================================

WITH source AS (
    SELECT * FROM {{ source('bronze', 'customers') }}
),

typed AS (
    SELECT
        customer_id,
        customer_unique_id,
        CAST(customer_zip_code_prefix AS VARCHAR(10)) AS customer_zip_code_prefix,
        LOWER(TRIM(customer_city))                    AS customer_city,
        UPPER(TRIM(customer_state))                   AS customer_state,
        _ingested_at,
        _source_file
    FROM source
    WHERE customer_id IS NOT NULL
      AND customer_unique_id IS NOT NULL
)

SELECT * FROM typed
