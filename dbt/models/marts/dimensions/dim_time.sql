-- models/marts/dimensions/dim_time.sql
-- ============================================================================
-- Dimension: time
-- Grain: one row per hour for the Olist data date range
-- Source: generated (no source table - pure SQL date spine)
-- Range: 2016-01-01 to 2018-12-31 (covers full Olist dataset)
--
-- Flattened design: all time attributes in one table
-- No separate dim_day or dim_month
-- Decision documented in DECISION_LOG.txt and DESIGN_AMENDMENTS.txt
--
-- time_sk format: YYYYMMDDHH integer (e.g. 2017100221 for 2017-10-02 21:00)
-- ============================================================================

{{
    config(
        materialized='table',
        schema='mart'
    )
}}

WITH date_spine AS (
    -- Generate one row per hour from 2016-01-01 to 2018-12-31
    -- Uses PostgreSQL generate_series
    SELECT
        generate_series(
            '2016-01-01 00:00:00'::TIMESTAMPTZ,
            '2018-12-31 23:00:00'::TIMESTAMPTZ,
            '1 hour'::INTERVAL
        ) AS ts
),

final AS (
    SELECT
        -- time_sk: YYYYMMDDHH integer format for fast joins
        CAST(
            TO_CHAR(ts, 'YYYYMMDDHH24')
        AS INTEGER)                                 AS time_sk,

        ts::DATE                                    AS date_actual,
        EXTRACT(YEAR FROM ts)::SMALLINT             AS year_number,
        EXTRACT(MONTH FROM ts)::SMALLINT            AS month_number,
        TO_CHAR(ts, 'Month')                        AS month_name,
        EXTRACT(QUARTER FROM ts)::SMALLINT          AS quarter_number,
        EXTRACT(DAY FROM ts)::SMALLINT              AS day_of_month,
        EXTRACT(DOW FROM ts)::SMALLINT              AS day_of_week,
        TO_CHAR(ts, 'Day')                          AS day_name,
        EXTRACT(HOUR FROM ts)::SMALLINT             AS hour_of_day,

        -- Weekend flag: Saturday (6) or Sunday (0)
        CASE WHEN EXTRACT(DOW FROM ts) IN (0, 6)
             THEN true ELSE false END               AS is_weekend,

        -- Business hours: 09:00-17:00 on weekdays
        CASE WHEN EXTRACT(DOW FROM ts) NOT IN (0, 6)
              AND EXTRACT(HOUR FROM ts) BETWEEN 9 AND 16
             THEN true ELSE false END               AS is_business_hours,

        -- Time band
        CASE
            WHEN EXTRACT(HOUR FROM ts) BETWEEN 0 AND 5  THEN 'early_morning'
            WHEN EXTRACT(HOUR FROM ts) BETWEEN 6 AND 11 THEN 'morning'
            WHEN EXTRACT(HOUR FROM ts) BETWEEN 12 AND 17 THEN 'afternoon'
            WHEN EXTRACT(HOUR FROM ts) BETWEEN 18 AND 21 THEN 'evening'
            ELSE 'night'
        END                                         AS time_band

    FROM date_spine
)

SELECT * FROM final
