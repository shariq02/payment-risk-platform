-- models/staging/stg_products.sql
-- ============================================================================
-- Staging: products
-- Source: bronze.products + dbt seed product_category_name_translation
-- Grain: one row per product (32,951 rows)
-- Changes from bronze:
--   - Olist typos corrected: lenght -> length
--   - All dimension columns cast from VARCHAR/DECIMAL to correct types
--   - English category names joined from seed translation table
--   - 2 categories missing translation use Portuguese name as fallback:
--     pc_gamer, portateis_cozinha_e_preparadores_de_alimentos
--   - 610 rows with null category kept as-is (no category available)
-- ============================================================================

WITH source AS (
    SELECT * FROM {{ source('bronze', 'products') }}
),

-- dbt seed: product_category_name_translation.csv
-- 71 rows mapping Portuguese category names to English
translation AS (
    SELECT
        product_category_name,
        product_category_name_english
    FROM {{ ref('product_category_name_translation') }}
),

typed AS (
    SELECT
        p.product_id,
        p.product_category_name                         AS product_category_name_portuguese,

        -- English name with fallback to Portuguese for 2 missing translations
        COALESCE(
            t.product_category_name_english,
            p.product_category_name
        )                                               AS product_category_name_english,

        -- Correct Olist typos (lenght -> length)
        CAST(p.product_name_lenght        AS INTEGER)  AS product_name_length,
        CAST(p.product_description_lenght AS INTEGER)  AS product_description_length,
        CAST(p.product_photos_qty         AS INTEGER)  AS product_photos_qty,
        CAST(p.product_weight_g           AS DECIMAL(10,1)) AS product_weight_g,
        CAST(p.product_length_cm          AS DECIMAL(6,1))  AS product_length_cm,
        CAST(p.product_height_cm          AS DECIMAL(6,1))  AS product_height_cm,
        CAST(p.product_width_cm           AS DECIMAL(6,1))  AS product_width_cm,

        p._ingested_at,
        p._source_file

    FROM source p
    LEFT JOIN translation t
        ON p.product_category_name = t.product_category_name
    WHERE p.product_id IS NOT NULL
)

SELECT * FROM typed
