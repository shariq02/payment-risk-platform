-- models/marts/dimensions/dim_product.sql
-- ============================================================================
-- Dimension: product
-- Grain: one row per product (32,951 rows)
-- Source: stg_products
-- Joins to: dim_product_category via English category name
--
-- No SCD2 - products do not change after listing
-- 610 products have null category - assigned to unknown category
-- English category names used throughout (from stg_products join to seed)
-- ============================================================================

{{
    config(
        materialized='table',
        schema='mart'
    )
}}

WITH products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

-- Get product category surrogate keys
categories AS (
    SELECT
        product_category_sk,
        category_code,
        category_name_english,
        category_name_portuguese
    FROM mart.dim_product_category
),

final AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY p.product_id) AS product_sk,
        p.product_id,
        COALESCE(c.product_category_sk, -1)     AS product_category_sk,
        p.product_name_length,
        p.product_description_length,
        p.product_photos_qty,
        p.product_weight_g,
        p.product_length_cm,
        p.product_height_cm,
        p.product_width_cm,
        p.product_category_name_english,
        p.product_category_name_portuguese
    FROM products p
    LEFT JOIN categories c
        ON p.product_category_name_english = c.category_name_english
)

SELECT * FROM final
WHERE product_id IS NOT NULL
