-- models/staging/stg_products.sql
-- ============================================================================
-- Staging: products
-- Source: bronze.products + dbt seed product_category_name_translation
-- Grain: one row per product (32,951 rows)
-- Changes from bronze:
--   - Olist typos corrected: lenght -> length
--   - NaN values in numeric columns converted to NULL before casting
--     610 rows have NaN in all dimension columns (no category data)
--   - English category names joined from seed translation table
--   - 2 categories missing translation use Portuguese name as fallback
-- ============================================================================

WITH source AS (
    SELECT * FROM {{ source('bronze', 'products') }}
),

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

        COALESCE(
            t.product_category_name_english,
            p.product_category_name
        )                                               AS product_category_name_english,

        -- NaN handling: Olist stores missing numeric values as NaN string
        -- NULLIF converts NaN to NULL before casting
        CASE WHEN p.product_name_lenght::TEXT IN ('NaN', '', 'nan')
             THEN NULL
             
             ELSE CAST(p.product_name_lenght AS DECIMAL(6,1))
        END::INTEGER                                    AS product_name_length,

        CASE WHEN p.product_description_lenght::TEXT IN ('NaN', '', 'nan')
             THEN NULL
             ELSE CAST(p.product_description_lenght AS DECIMAL(8,1))
        END::INTEGER                                    AS product_description_length,

        CASE WHEN p.product_photos_qty::TEXT IN ('NaN', '', 'nan')
             THEN NULL
             ELSE CAST(p.product_photos_qty AS DECIMAL(4,1))
        END::INTEGER                                    AS product_photos_qty,

        CASE WHEN p.product_weight_g::TEXT IN ('NaN', '', 'nan')
             THEN NULL
             ELSE CAST(p.product_weight_g AS DECIMAL(10,1))
        END                                             AS product_weight_g,

        CASE WHEN p.product_length_cm::TEXT IN ('NaN', '', 'nan')
             THEN NULL
             ELSE CAST(p.product_length_cm AS DECIMAL(6,1))
        END                                             AS product_length_cm,

        CASE WHEN p.product_height_cm::TEXT IN ('NaN', '', 'nan')
             THEN NULL
             ELSE CAST(p.product_height_cm AS DECIMAL(6,1))
        END                                             AS product_height_cm,

        CASE WHEN p.product_width_cm::TEXT IN ('NaN', '', 'nan')
             THEN NULL
             ELSE CAST(p.product_width_cm AS DECIMAL(6,1))
        END                                             AS product_width_cm,

        p._ingested_at,
        p._source_file

    FROM source p
    LEFT JOIN translation t
        ON p.product_category_name = t.product_category_name
    WHERE p.product_id IS NOT NULL
)

SELECT * FROM typed
