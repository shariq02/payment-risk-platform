-- models/staging/stg_sellers.sql
-- ============================================================================
-- Staging: sellers
-- Source: bronze.sellers
-- Grain: one row per seller (3,095 rows)
-- Changes from bronze:
--   - seller_zip_code_prefix cast to VARCHAR (already VARCHAR, kept clean)
--   - city and state trimmed and lowercased for consistency
-- ============================================================================

WITH source AS (
    SELECT * FROM {{ source('bronze', 'sellers') }}
),

typed AS (
    SELECT
        seller_id,
        CAST(seller_zip_code_prefix AS VARCHAR(10)) AS seller_zip_code_prefix,
        LOWER(TRIM(seller_city))                    AS seller_city,
        UPPER(TRIM(seller_state))                   AS seller_state,
        _ingested_at,
        _source_file
    FROM source
    WHERE seller_id IS NOT NULL
)

SELECT * FROM typed
