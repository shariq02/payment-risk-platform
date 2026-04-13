"""
Bronze Layer Verification Script
=================================
Verifies bronze tables are loaded correctly after running load_bronze.py.
Checks row counts, duplicates, and audit column population.

Usage:
    python3.11 verify_bronze.py

Place at: tests/integration/verify_bronze.py
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

EXPECTED_ROW_COUNTS = {
    'orders':           99441,
    'order_items':      112650,
    'order_payments':   103886,
    'customers':        99441,
    'sellers':          3095,
    'products':         32951,
    'reviews':          99224,
    'geolocation':      1000163,
}

# Tables with single primary keys - checked for duplicates
SINGLE_PK_TABLES = {
    'orders':    'order_id',
    'customers': 'customer_id',
    'sellers':   'seller_id',
    'products':  'product_id',
}

# Tables with composite primary keys - checked for duplicates
COMPOSITE_PK_TABLES = {
    'order_items':     ['order_id', 'order_item_id'],
    'order_payments':  ['order_id', 'payment_sequential'],
    'reviews':         ['review_id', 'order_id'],
}

# Geolocation has no PK - only row count checked


# ============================================================================
# VERIFICATION CHECKS
# ============================================================================

def check_row_counts(cur) -> list:
    """Verify row counts match expected values."""
    results = []
    for table, expected in EXPECTED_ROW_COUNTS.items():
        cur.execute(f"SELECT COUNT(*) FROM bronze.{table}")
        actual = cur.fetchone()[0]
        passed = actual == expected
        results.append({
            'check': f"row_count_{table}",
            'expected': f"{expected:,}",
            'actual': f"{actual:,}",
            'passed': passed,
        })
    return results


def check_duplicates(cur) -> list:
    """Verify no duplicate primary keys exist."""
    results = []

    # Single PK tables
    for table, pk in SINGLE_PK_TABLES.items():
        cur.execute(f"""
            SELECT COUNT(*) - COUNT(DISTINCT {pk})
            FROM bronze.{table}
        """)
        duplicates = cur.fetchone()[0]
        results.append({
            'check': f"duplicates_{table}",
            'expected': '0',
            'actual': str(duplicates),
            'passed': duplicates == 0,
        })

    # Composite PK tables
    for table, pks in COMPOSITE_PK_TABLES.items():
        pk_cols = ', '.join(pks)
        cur.execute(f"""
            SELECT COUNT(*) - COUNT(DISTINCT ({pk_cols}))
            FROM bronze.{table}
        """)
        duplicates = cur.fetchone()[0]
        results.append({
            'check': f"duplicates_{table}",
            'expected': '0',
            'actual': str(duplicates),
            'passed': duplicates == 0,
        })

    return results


def check_audit_columns(cur) -> list:
    """Verify _ingested_at is populated on all rows in all tables."""
    results = []
    for table in EXPECTED_ROW_COUNTS:
        cur.execute(f"""
            SELECT COUNT(*)
            FROM bronze.{table}
            WHERE _ingested_at IS NULL
        """)
        null_count = cur.fetchone()[0]
        results.append({
            'check': f"ingested_at_nulls_{table}",
            'expected': '0',
            'actual': str(null_count),
            'passed': null_count == 0,
        })
    return results


def check_payment_types(cur) -> list:
    """Verify payment types match known values from exploration."""
    cur.execute("""
        SELECT DISTINCT payment_type
        FROM bronze.order_payments
        ORDER BY payment_type
    """)
    actual_types = [row[0] for row in cur.fetchall()]
    expected_types = ['boleto', 'credit_card', 'debit_card', 'not_defined', 'voucher']
    passed = set(actual_types) == set(expected_types)
    return [{
        'check': 'payment_types',
        'expected': str(expected_types),
        'actual': str(actual_types),
        'passed': passed,
    }]


def check_review_scores(cur) -> list:
    """Verify review scores are in valid range 1-5."""
    cur.execute("""
        SELECT COUNT(*)
        FROM bronze.reviews
        WHERE review_score < 1 OR review_score > 5
    """)
    invalid = cur.fetchone()[0]
    return [{
        'check': 'review_score_range',
        'expected': '0 invalid scores',
        'actual': f"{invalid} invalid scores",
        'passed': invalid == 0,
    }]


# ============================================================================
# MAIN
# ============================================================================

def run():
    logger.info("=" * 60)
    logger.info("Bronze Layer Verification - Starting")
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
        all_results += check_row_counts(cur)
        all_results += check_duplicates(cur)
        all_results += check_audit_columns(cur)
        all_results += check_payment_types(cur)
        all_results += check_review_scores(cur)
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
        logger.error("Bronze verification FAILED")
        sys.exit(1)
    else:
        logger.info("Bronze verification PASSED - safe to proceed to Phase 3")
    logger.info("=" * 60)


if __name__ == '__main__':
    run()
