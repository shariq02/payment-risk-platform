-- models/staging/stg_geolocation.sql
-- ============================================================================
-- Staging: geolocation
-- Source: bronze.geolocation
-- Grain: one row per zip code prefix (19,015 unique zips from 1,000,163 rows)
-- Changes from bronze:
--   - Averaged lat/lng per zip code prefix
--   - city and state taken from first occurrence per zip
--   - Multiple rows per zip collapsed to single representative row
--
-- Decision: Use average coordinates per zip rather than picking one row.
-- This gives a more representative center point for the zip area.
-- Documented in DECISION_LOG.txt.
-- ============================================================================

WITH source AS (
    SELECT * FROM {{ source('bronze', 'geolocation') }}
),

averaged AS (
    SELECT
        CAST(geolocation_zip_code_prefix AS VARCHAR(10)) AS zip_code_prefix,
        AVG(CAST(geolocation_lat AS DECIMAL(10,6)))      AS avg_lat,
        AVG(CAST(geolocation_lng AS DECIMAL(10,6)))      AS avg_lng,
        -- First city and state per zip (consistent within zip in practice)
        MIN(LOWER(TRIM(geolocation_city)))               AS city_name,
        MIN(UPPER(TRIM(geolocation_state)))              AS state_code
    FROM source
    WHERE geolocation_zip_code_prefix IS NOT NULL
    GROUP BY geolocation_zip_code_prefix
)

SELECT * FROM averaged
