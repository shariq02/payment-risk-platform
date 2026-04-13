-- models/marts/facts/risk_kpis_daily.sql
-- ============================================================================
-- Aggregate: daily risk KPIs
-- Grain: one row per date
-- Source: fact_order_payments + fact_order_fulfillment
-- Refreshed: every 30 minutes by Airflow DAG 2
--
-- Pre-aggregated for dashboard and API performance.
-- Decision documented in DECISION_LOG.txt:
--   Dashboard queries on pre-aggregated daily rows are orders of magnitude
--   faster than scanning both fact tables on every request.
--   At 100k orders this is not critical but demonstrates production thinking.
-- ============================================================================

{{
    config(
        materialized='table',
        schema='mart',
        unique_key='date_actual'
    )
}}

WITH payments AS (
    SELECT * FROM {{ ref('fact_order_payments') }}
),

fulfillment AS (
    SELECT * FROM {{ ref('fact_order_fulfillment') }}
),

-- Payment KPIs aggregated by date
payment_kpis AS (
    SELECT
        event_ts::DATE                              AS date_actual,
        COUNT(*)                                    AS total_payment_transactions,
        SUM(payment_value)                          AS total_payment_value,
        AVG(payment_value)                          AS avg_payment_value,
        COUNT(CASE WHEN is_dispute_risk
              THEN 1 END)                           AS dispute_risk_payment_count,
        CASE WHEN COUNT(*) > 0
             THEN COUNT(CASE WHEN is_dispute_risk THEN 1 END)::DECIMAL
                  / COUNT(*)
             ELSE 0 END                             AS dispute_risk_payment_rate,
        AVG(payment_risk_score)                     AS avg_payment_risk_score,
        COUNT(CASE WHEN is_high_value
              THEN 1 END)                           AS high_value_payment_count,
        COUNT(CASE WHEN order_status = 'credit_card'
              THEN 1 END)                           AS credit_card_count,
        COUNT(CASE WHEN order_status = 'boleto'
              THEN 1 END)                           AS boleto_count,
        COUNT(CASE WHEN order_status = 'voucher'
              THEN 1 END)                           AS voucher_count,
        COUNT(CASE WHEN order_status = 'debit_card'
              THEN 1 END)                           AS debit_card_count
    FROM payments
    WHERE event_ts IS NOT NULL
    GROUP BY event_ts::DATE
),

-- Fulfillment KPIs aggregated by date
fulfillment_kpis AS (
    SELECT
        event_ts::DATE                              AS date_actual,
        COUNT(*)                                    AS total_order_items,
        SUM(item_price)                             AS total_item_value,
        COUNT(CASE WHEN is_late_delivery
              THEN 1 END)                           AS late_delivery_count,
        CASE WHEN COUNT(*) > 0
             THEN COUNT(CASE WHEN is_late_delivery THEN 1 END)::DECIMAL
                  / COUNT(*)
             ELSE 0 END                             AS late_delivery_rate,
        COUNT(CASE WHEN is_cancelled
              THEN 1 END)                           AS cancellation_count,
        CASE WHEN COUNT(*) > 0
             THEN COUNT(CASE WHEN is_cancelled THEN 1 END)::DECIMAL
                  / COUNT(*)
             ELSE 0 END                             AS cancellation_rate,
        AVG(fulfillment_risk_score)                 AS avg_fulfillment_risk_score,
        COUNT(CASE WHEN is_dispute_proxy
              THEN 1 END)                           AS dispute_proxy_count,
        AVG(review_score)                           AS avg_review_score
    FROM fulfillment
    WHERE event_ts IS NOT NULL
    GROUP BY event_ts::DATE
),

-- Get distinct dates across both facts
all_dates AS (
    SELECT date_actual FROM payment_kpis
    UNION
    SELECT date_actual FROM fulfillment_kpis
),

final AS (
    SELECT
        d.date_actual,

        -- Payment KPIs
        COALESCE(p.total_payment_transactions, 0)   AS total_payment_transactions,
        COALESCE(p.total_payment_value, 0)          AS total_payment_value,
        COALESCE(p.avg_payment_value, 0)            AS avg_payment_value,
        COALESCE(p.dispute_risk_payment_count, 0)   AS dispute_risk_payment_count,
        COALESCE(p.dispute_risk_payment_rate, 0)    AS dispute_risk_payment_rate,
        COALESCE(p.avg_payment_risk_score, 0)       AS avg_payment_risk_score,
        COALESCE(p.high_value_payment_count, 0)     AS high_value_payment_count,
        COALESCE(p.credit_card_count, 0)            AS credit_card_count,
        COALESCE(p.boleto_count, 0)                 AS boleto_count,
        COALESCE(p.voucher_count, 0)                AS voucher_count,
        COALESCE(p.debit_card_count, 0)             AS debit_card_count,

        -- Fulfillment KPIs
        COALESCE(f.total_order_items, 0)            AS total_order_items,
        COALESCE(f.total_item_value, 0)             AS total_item_value,
        COALESCE(f.late_delivery_count, 0)          AS late_delivery_count,
        COALESCE(f.late_delivery_rate, 0)           AS late_delivery_rate,
        COALESCE(f.cancellation_count, 0)           AS cancellation_count,
        COALESCE(f.cancellation_rate, 0)            AS cancellation_rate,
        COALESCE(f.avg_fulfillment_risk_score, 0)   AS avg_fulfillment_risk_score,
        COALESCE(f.dispute_proxy_count, 0)          AS dispute_proxy_count,
        COALESCE(f.avg_review_score, 0)             AS avg_review_score,

        NOW()                                       AS computed_at

    FROM all_dates d
    LEFT JOIN payment_kpis p ON d.date_actual = p.date_actual
    LEFT JOIN fulfillment_kpis f ON d.date_actual = f.date_actual
)

SELECT * FROM final
ORDER BY date_actual
