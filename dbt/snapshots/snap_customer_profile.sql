{% snapshot snap_customer_profile %}

{{
    config(
      target_schema='snapshots',
      unique_key='customer_unique_id',
      strategy='check',
      check_cols=['customer_city', 'customer_state', 'segment_code', 'risk_tier_code',
                  'total_orders', 'total_payment_value', 'avg_payment_value',
                  'preferred_payment_type', 'avg_installments', 'has_dispute_history',
                  'cancelled_orders'],
      invalidate_hard_deletes=True
    )
}}

SELECT
    customer_unique_id,
    segment_code,
    risk_tier_code,
    total_orders,
    total_payment_value,
    avg_payment_value,
    preferred_payment_type,
    avg_installments,
    has_dispute_history,
    cancelled_orders,
    first_order_ts,
    last_order_ts,
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM {{ ref('dim_customer') }}

{% endsnapshot %}
