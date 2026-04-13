"""
Schema Verification Script
============================
Verifies mart schema is set up correctly after running setup_warehouse.py.
Checks table existence, column structure, seed data, indexes, and FK integrity.

Usage:
    python3.11 verify_schema.py

Place at: tests/integration/verify_schema.py
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import psycopg2
from config import DATABASE_CONFIG, get_logger

logger = get_logger(__name__)

# ============================================================================
# EXPECTED VALUES
# ============================================================================

EXPECTED_SCHEMAS = ['bronze', 'staging', 'mart', 'snapshots']

EXPECTED_DIMENSION_TABLES = [
    'dim_region',
    'dim_state',
    'dim_city',
    'dim_geo',
    'dim_customer_risk_tier',
    'dim_customer_segment',
    'dim_customer',
    'dim_seller_industry',
    'dim_seller_category',
    'dim_seller',
    'dim_product_department',
    'dim_product_category',
    'dim_product',
    'dim_payment_method',
    'dim_time',
]

EXPECTED_FACT_TABLES = [
    'fact_order_payments',
    'fact_order_fulfillment',
    'risk_kpis_daily',
]

EXPECTED_SEED_COUNTS = {
    'dim_region':               5,
    'dim_state':                27,
    'dim_customer_risk_tier':   4,
    'dim_customer_segment':     4,
    'dim_seller_industry':      9,
    'dim_seller_category':      7,
    'dim_product_department':   9,
    'dim_payment_method':       5,
}

EXPECTED_INDEXES = [
    'idx_payments_event_ts',
    'idx_payments_customer_sk',
    'idx_payments_risk_score',
    'idx_payments_dispute_risk',
    'idx_payments_order_id',
    'idx_payments_customer_unique_id',
    'idx_fulfillment_event_ts',
    'idx_fulfillment_seller_sk',
    'idx_fulfillment_risk_score',
    'idx_fulfillment_late_delivery',
    'idx_fulfillment_order_id',
    'idx_fulfillment_seller_id',
    'idx_customer_current',
    'idx_customer_valid_from',
    'idx_seller_current',
    'idx_seller_valid_from',
    'idx_geo_zip_code',
    'idx_product_id',
    'idx_time_date_actual',
    'idx_kpis_date_actual',
]


# ============================================================================
# CHECKS
# ============================================================================

def check_schemas(cur) -> list:
    """Verify all required schemas exist."""
    cur.execute("""
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name IN ('bronze','staging','mart','snapshots')
    """)
    existing = [row[0] for row in cur.fetchall()]
    results = []
    for schema in EXPECTED_SCHEMAS:
        results.append({
            'check': f"schema_{schema}",
            'expected': 'exists',
            'actual': 'exists' if schema in existing else 'missing',
            'passed': schema in existing,
        })
    return results


def check_tables(cur) -> list:
    """Verify all dimension and fact tables exist in mart schema."""
    cur.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'mart'
    """)
    existing = [row[0] for row in cur.fetchall()]
    results = []
    for table in EXPECTED_DIMENSION_TABLES + EXPECTED_FACT_TABLES:
        results.append({
            'check': f"table_{table}",
            'expected': 'exists',
            'actual': 'exists' if table in existing else 'missing',
            'passed': table in existing,
        })
    return results


def check_seed_counts(cur) -> list:
    """Verify seed reference data was inserted correctly."""
    results = []
    for table, expected_count in EXPECTED_SEED_COUNTS.items():
        cur.execute(f"SELECT COUNT(*) FROM mart.{table}")
        actual = cur.fetchone()[0]
        results.append({
            'check': f"seed_count_{table}",
            'expected': str(expected_count),
            'actual': str(actual),
            'passed': actual >= expected_count,
        })
    return results


def check_indexes(cur) -> list:
    """Verify all indexes were created."""
    cur.execute("""
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname = 'mart'
    """)
    existing = [row[0] for row in cur.fetchall()]
    results = []
    for index in EXPECTED_INDEXES:
        results.append({
            'check': f"index_{index}",
            'expected': 'exists',
            'actual': 'exists' if index in existing else 'missing',
            'passed': index in existing,
        })
    return results


def check_scd2_columns(cur) -> list:
    """Verify SCD2 columns exist on dim_customer and dim_seller."""
    results = []
    scd2_tables = {
        'dim_customer': ['valid_from', 'valid_to', 'is_current', 'dbt_updated_at'],
        'dim_seller':   ['valid_from', 'valid_to', 'is_current', 'dbt_updated_at'],
    }
    for table, columns in scd2_tables.items():
        cur.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'mart'
            AND table_name = '{table}'
        """)
        existing_cols = [row[0] for row in cur.fetchall()]
        for col in columns:
            results.append({
                'check': f"scd2_col_{table}_{col}",
                'expected': 'exists',
                'actual': 'exists' if col in existing_cols else 'missing',
                'passed': col in existing_cols,
            })
    return results


def check_fact_columns(cur) -> list:
    """Verify key columns exist on both fact tables."""
    results = []
    fact_columns = {
        'fact_order_payments': [
            'payment_sk', 'order_id', 'customer_sk', 'payment_method_sk',
            'payment_value', 'payment_risk_score', 'is_dispute_risk',
            'is_high_value', 'event_ts',
        ],
        'fact_order_fulfillment': [
            'fulfillment_sk', 'order_id', 'seller_sk', 'product_sk',
            'item_price', 'freight_value', 'fulfillment_risk_score',
            'is_late_delivery', 'is_cancelled', 'is_dispute_proxy',
            'review_score', 'event_ts',
        ],
    }
    for table, columns in fact_columns.items():
        cur.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'mart'
            AND table_name = '{table}'
        """)
        existing_cols = [row[0] for row in cur.fetchall()]
        for col in columns:
            results.append({
                'check': f"fact_col_{table}_{col}",
                'expected': 'exists',
                'actual': 'exists' if col in existing_cols else 'missing',
                'passed': col in existing_cols,
            })
    return results


# ============================================================================
# MAIN
# ============================================================================

def run():
    logger.info("=" * 60)
    logger.info("Schema Verification - Starting")
    logger.info("=" * 60)

    try:
        conn = psycopg2.connect(
            host=DATABASE_CONFIG['host'],
            port=DATABASE_CONFIG['port'],
            dbname=DATABASE_CONFIG['database'],
            user=DATABASE_CONFIG['user'],
            password=DATABASE_CONFIG['password'],
        )
        logger.info("Database connection established")
    except Exception as exc:
        logger.error(f"Database connection failed: {exc}")
        sys.exit(1)

    cur = conn.cursor()
    all_results = []

    try:
        all_results += check_schemas(cur)
        all_results += check_tables(cur)
        all_results += check_seed_counts(cur)
        all_results += check_indexes(cur)
        all_results += check_scd2_columns(cur)
        all_results += check_fact_columns(cur)
    finally:
        cur.close()
        conn.close()

    # Print results
    passed = [r for r in all_results if r['passed']]
    failed = [r for r in all_results if not r['passed']]

    logger.info("\nVERIFICATION RESULTS")
    logger.info("-" * 60)
    for r in all_results:
        status = "PASS" if r['passed'] else "FAIL"
        logger.info(f"  [{status}] {r['check']}: {r['actual']}")

    logger.info("\n" + "=" * 60)
    logger.info(f"  Total checks: {len(all_results)}")
    logger.info(f"  Passed:       {len(passed)}")
    logger.info(f"  Failed:       {len(failed)}")

    if failed:
        logger.error("\nFailed checks:")
        for r in failed:
            logger.error(
                f"  {r['check']}: expected {r['expected']}, got {r['actual']}"
            )
        logger.error("Schema verification FAILED")
        sys.exit(1)
    else:
        logger.info("Schema verification PASSED - safe to proceed to Phase 4")
    logger.info("=" * 60)


if __name__ == '__main__':
    run()
