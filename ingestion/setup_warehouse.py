"""
Warehouse Setup Script
======================
Creates the full warehouse schema by executing DDL files in order.
Checks if mart tables already exist before running - skips if already set up.

DDL files executed in order:
  02_create_staging_schema.sql   - schemas
  03_create_mart_dimensions.sql  - dimension tables
  04_create_mart_facts.sql       - fact tables and aggregates
  05_create_indexes.sql          - all indexes
  06_seed_reference_data.sql     - reference dimension data

Usage:
    python3.11 setup_warehouse.py

Place at: ingestion/setup_warehouse.py
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import psycopg2
from config import DATABASE_CONFIG, SQL_DDL_DIR, get_logger

logger = get_logger(__name__)

# DDL files to execute in strict order
DDL_FILES = [
    '02_create_staging_schema.sql',
    '03_create_mart_dimensions.sql',
    '04_create_mart_facts.sql',
    '05_create_indexes.sql',
    '06_seed_reference_data.sql',
]

# Check these tables exist to determine if setup already ran
MART_SENTINEL_TABLES = [
    'dim_region',
    'dim_customer',
    'dim_seller',
    'dim_product',
    'dim_time',
    'dim_payment_method',
    'fact_order_payments',
    'fact_order_fulfillment',
    'risk_kpis_daily',
]


def get_connection():
    return psycopg2.connect(
        host=DATABASE_CONFIG['host'],
        port=DATABASE_CONFIG['port'],
        dbname=DATABASE_CONFIG['database'],
        user=DATABASE_CONFIG['user'],
        password=DATABASE_CONFIG['password'],
    )


def mart_tables_exist(conn) -> bool:
    """Check if all expected mart tables already exist."""
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'mart'
        """)
        existing = [row[0] for row in cur.fetchall()]
        missing = [t for t in MART_SENTINEL_TABLES if t not in existing]
        if missing:
            logger.info(f"Missing mart tables: {missing}")
            return False
        logger.info("All mart tables already exist")
        return True
    finally:
        cur.close()


def execute_ddl_file(conn, filename: str):
    """Read and execute a single DDL file."""
    filepath = SQL_DDL_DIR / filename

    if not filepath.exists():
        raise FileNotFoundError(
            f"DDL file not found: {filepath}"
        )

    logger.info(f"Executing: {filename}")
    sql = filepath.read_text(encoding='utf-8')

    cur = conn.cursor()
    try:
        cur.execute(sql)
        conn.commit()
        logger.info(f"  {filename} - OK")
    except Exception as exc:
        conn.rollback()
        logger.error(f"  {filename} - FAILED: {exc}")
        raise
    finally:
        cur.close()


def run():
    logger.info("=" * 60)
    logger.info("Warehouse Setup - Starting")
    logger.info("=" * 60)

    # Verify DDL files exist before connecting
    missing_files = [f for f in DDL_FILES if not (SQL_DDL_DIR / f).exists()]
    if missing_files:
        logger.error(f"Missing DDL files: {missing_files}")
        logger.error(f"Expected in: {SQL_DDL_DIR}")
        sys.exit(1)

    logger.info("All DDL files found")

    # Connect
    try:
        conn = get_connection()
        logger.info("Database connection established")
    except Exception as exc:
        logger.error(f"Database connection failed: {exc}")
        sys.exit(1)

    try:
        # Check if already set up
        if mart_tables_exist(conn):
            logger.info("Warehouse already set up - skipping DDL execution")
            logger.info("To force re-run: drop mart schema tables manually first")
            return

        # Execute DDL files in order
        logger.info("\nRunning DDL files in order:")
        for filename in DDL_FILES:
            execute_ddl_file(conn, filename)

        logger.info("\n" + "=" * 60)
        logger.info("Warehouse Setup - Complete")
        logger.info("All schemas, tables, indexes, and seed data created")
        logger.info("=" * 60)

    except Exception as exc:
        logger.error(f"Warehouse setup failed: {exc}")
        sys.exit(1)
    finally:
        conn.close()


if __name__ == '__main__':
    run()
