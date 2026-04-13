{% snapshot snap_seller_risk_profile %}

{{
    config(
      target_schema='snapshots',
      unique_key='seller_id',
      strategy='check',
      check_cols=['seller_city', 'seller_state', 'risk_tier', 'seller_reliability_score',
                  'is_new_seller', 'total_orders_30d', 'late_delivery_rate_30d',
                  'avg_review_score_30d', 'cancellation_rate_30d', 'insufficient_data'],
      invalidate_hard_deletes=True
    )
}}

SELECT
    seller_id,
    risk_tier,
    seller_reliability_score,
    is_new_seller,
    total_orders_30d,
    late_delivery_rate_30d,
    avg_review_score_30d,
    cancellation_rate_30d,
    insufficient_data,
    first_order_ts,
    seller_zip_code_prefix,
    seller_city,
    seller_state
FROM {{ ref('dim_seller') }}

{% endsnapshot %}
