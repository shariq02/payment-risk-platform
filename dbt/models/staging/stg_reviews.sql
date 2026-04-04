-- models/staging/stg_reviews.sql
-- ============================================================================
-- Staging: reviews
-- Source: bronze.reviews
-- Grain: one row per ORDER (deduplicated from 99,224 to one per order)
-- Changes from bronze:
--   - Deduplicated by order_id keeping latest review_creation_date
--   - review_id not used as key (814 duplicates found in exploration)
--   - Timestamps cast from VARCHAR to TIMESTAMPTZ
--   - is_dispute_proxy flag added: review_score <= 2
--   - 543 orders had 2 reviews, 4 had 3 reviews - latest kept
-- ============================================================================

WITH source AS (
    SELECT * FROM {{ source('bronze', 'reviews') }}
),

typed AS (
    SELECT
        review_id,
        order_id,
        CAST(review_score AS SMALLINT)               AS review_score,
        review_comment_title,
        review_comment_message,
        CAST(review_creation_date   AS TIMESTAMPTZ)  AS review_creation_ts,
        CAST(review_answer_timestamp AS TIMESTAMPTZ) AS review_answer_ts,

        -- Dispute proxy flag
        -- review_score <= 2 used as proxy for dispute or dissatisfaction
        -- Documented assumption: not a confirmed chargeback
        CASE WHEN CAST(review_score AS SMALLINT) <= 2
             THEN true ELSE false END                 AS is_dispute_proxy,

        _ingested_at,
        _source_file,

        -- Row number for deduplication
        -- Keep latest review per order by review_creation_date
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY CAST(review_creation_date AS TIMESTAMPTZ) DESC
        )                                             AS rn

    FROM source
    WHERE order_id IS NOT NULL
      AND review_score IS NOT NULL
),

-- Keep only the latest review per order
deduplicated AS (
    SELECT
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_ts,
        review_answer_ts,
        is_dispute_proxy,
        _ingested_at,
        _source_file
    FROM typed
    WHERE rn = 1
)

SELECT * FROM deduplicated
