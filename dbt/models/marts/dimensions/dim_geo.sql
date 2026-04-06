-- models/marts/dimensions/dim_geo.sql
-- ============================================================================
-- Dimension: geography
-- Grain: one row per zip code prefix (19,015 unique zips)
-- Source: stg_geolocation
-- ============================================================================

{{
    config(
        materialized='table',
        schema='mart'
    )
}}

WITH geo AS (
    SELECT * FROM {{ ref('stg_geolocation') }}
),

states AS (
    SELECT state_sk, state_code
    FROM mart.dim_state
),

cities AS (
    SELECT city_sk, state_sk, city_name
    FROM mart.dim_city
),

geo_with_state AS (
    SELECT
        g.zip_code_prefix,
        g.avg_lat,
        g.avg_lng,
        g.city_name,
        g.state_code,
        s.state_sk
    FROM geo g
    LEFT JOIN states s ON g.state_code = s.state_code
),

final AS (
    SELECT
        -- Surrogate key generated from row number
        ROW_NUMBER() OVER (ORDER BY gs.zip_code_prefix) AS geo_sk,
        gs.zip_code_prefix,
        COALESCE(c.city_sk, -1)                         AS city_sk,
        gs.avg_lat,
        gs.avg_lng,
        gs.city_name,
        gs.state_code,
        gs.state_sk
    FROM geo_with_state gs
    LEFT JOIN cities c
        ON gs.state_sk = c.state_sk
        AND LOWER(TRIM(gs.city_name)) = LOWER(TRIM(c.city_name))
)

SELECT * FROM final
WHERE zip_code_prefix IS NOT NULL
