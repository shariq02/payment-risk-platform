"""
Bronze Layer Loader
===================
Loads all Olist CSV files into PostgreSQL bronze schema.

Workflow:
  1. Check if bronze tables exist
  2. If not: execute 01_create_bronze_schema.sql to create them
  3. Load each CSV file into its corresponding bronze table
  4. Log row counts and any errors

Design principles:
  - Idempotent: safe to run multiple times
  - Faithful: no transformations, raw data only
  - Auditable: every row gets _ingested_at and _source_file columns
  - Logged: all errors written to logs/error.log automatically

Usage:
  python3.11 load_bronze.py
"""

import os
import sys
from pathlib import Path

# Add project root to path so config.py can be imported
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone

from config import (
    DATABASE_CONFIG,
    RAW_DATA_DIR,
    SQL_DDL_DIR,
    get_logger,
)

logger = get_logger(__name__)

# ============================================================================
# FILE TO TABLE MAPPING
# ============================================================================
# Maps each CSV filename to its bronze table name and primary key(s)
# Primary keys used for deduplication before insert
FILE_TABLE_MAP = {
    'olist_orders_dataset.csv': {
        'table': 'orders',
        'primary_keys': ['order_id'],
    },
    'olist_order_items_dataset.csv': {
        'table': 'order_items',
        'primary_keys': ['order_id', 'order_item_id'],
    },
    'olist_order_payments_dataset.csv': {
        'table': 'order_payments',
        'primary_keys': ['order_id', 'payment_sequential'],
    },
    'olist_customers_dataset.csv': {
        'table': 'customers',
        'primary_keys': ['customer_id'],
    },
    'olist_sellers_dataset.csv': {
        'table': 'sellers',
        'primary_keys': ['seller_id'],
    },
    'olist_products_dataset.csv': {
        'table': 'products',
        'primary_keys': ['product_id'],
    },
    'olist_order_reviews_dataset.csv': {
        'table': 'reviews',
        'primary_keys': ['review_id', 'order_id'],
    },
    'olist_geolocation_dataset.csv': {
        'table': 'geolocation',
        'primary_keys': [],  # No PK - geolocation is not unique by design
    },
}

# Expected row counts from data exploration
# Used for post-load verification
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


# ============================================================================
# DATABASE CONNECTION
# ============================================================================

def get_connection():
    """Create and return a PostgreSQL connection."""
    return psycopg2.connect(
        host=DATABASE_CONFIG['host'],
        port=DATABASE_CONFIG['port'],
        dbname=DATABASE_CONFIG['database'],
        user=DATABASE_CONFIG['user'],
        password=DATABASE_CONFIG['password'],
    )


# ============================================================================
# SCHEMA SETUP
# ============================================================================

def bronze_tables_exist(conn) -> bool:
    """
    Check if bronze tables already exist in the database.
    Returns True if all expected tables exist, False if any are missing.
    """
    expected_tables = list(FILE_TABLE_MAP[f]['table'] for f in FILE_TABLE_MAP)
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'bronze'
        """)
        existing_tables = [row[0] for row in cur.fetchall()]
        missing = [t for t in expected_tables if t not in existing_tables]
        if missing:
            logger.info(f"Missing bronze tables: {missing}")
            return False
        logger.info("All bronze tables already exist")
        return True
    finally:
        cur.close()


def create_bronze_schema(conn):
    """
    Execute 01_create_bronze_schema.sql to create all bronze tables.
    Called only if tables do not already exist.
    """
    ddl_file = SQL_DDL_DIR / '01_create_bronze_schema.sql'

    if not ddl_file.exists():
        raise FileNotFoundError(
            f"DDL file not found: {ddl_file}\n"
            f"Expected at: sql/ddl/01_create_bronze_schema.sql"
        )

    logger.info(f"Reading DDL from: {ddl_file}")
    ddl_sql = ddl_file.read_text(encoding='utf-8')

    cur = conn.cursor()
    try:
        cur.execute(ddl_sql)
        conn.commit()
        logger.info("Bronze schema created successfully from DDL file")
    except Exception as exc:
        conn.rollback()
        logger.error(f"Failed to create bronze schema: {exc}")
        raise
    finally:
        cur.close()


# ============================================================================
# DATA LOADING
# ============================================================================

def read_csv(filepath: Path, table: str) -> pd.DataFrame:
    """
    Read a CSV file into a DataFrame.
    Handles encoding issues common in Olist dataset.
    """
    logger.info(f"Reading: {filepath.name}")
    try:
        df = pd.read_csv(
            filepath,
            encoding='utf-8',
            dtype=str,  # Read everything as string first, cast in DDL
            keep_default_na=False,  # Keep empty strings as empty, not NaN
            na_values=[''],  # Only empty strings become NaN
        )
        logger.info(f"  Read {len(df):,} rows from {filepath.name}")
        return df
    except UnicodeDecodeError:
        # Fallback encoding for files with special characters
        logger.warning(f"  UTF-8 failed for {filepath.name}, trying latin-1")
        df = pd.read_csv(
            filepath,
            encoding='latin-1',
            dtype=str,
            keep_default_na=False,
            na_values=[''],
        )
        logger.info(f"  Read {len(df):,} rows using latin-1 encoding")
        return df


def deduplicate(df: pd.DataFrame, primary_keys: list, table: str) -> pd.DataFrame:
    """
    Deduplicate DataFrame by primary keys.
    Keeps last occurrence if duplicates exist.
    Skips deduplication for geolocation (no primary keys).
    """
    if not primary_keys:
        return df

    before = len(df)
    df = df.drop_duplicates(subset=primary_keys, keep='last')
    after = len(df)

    if before != after:
        logger.warning(
            f"  Deduplication removed {before - after:,} rows from {table}"
        )
    return df


def add_audit_columns(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    """Add _ingested_at and _source_file audit columns to DataFrame."""
    df['_ingested_at'] = datetime.now(timezone.utc).isoformat()
    df['_source_file'] = source_file
    return df


def load_table(conn, df: pd.DataFrame, table: str, primary_keys: list):
    """
    Load DataFrame into bronze table using INSERT ON CONFLICT DO NOTHING.
    This makes the load idempotent - existing rows are skipped, not duplicated.
    Geolocation uses plain INSERT (no conflict target - no PK).
    """
    schema_table = f"bronze.{table}"
    columns = list(df.columns)
    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]

    cur = conn.cursor()
    try:
        if primary_keys:
            conflict_target = ', '.join(primary_keys)
            sql = f"""
                INSERT INTO {schema_table} ({', '.join(columns)})
                VALUES %s
                ON CONFLICT ({conflict_target}) DO NOTHING
            """
        else:
            # Geolocation - no conflict handling, truncate and reload
            cur.execute(f"TRUNCATE TABLE {schema_table}")
            sql = f"""
                INSERT INTO {schema_table} ({', '.join(columns)})
                VALUES %s
            """

        execute_values(cur, sql, rows, page_size=1000)
        conn.commit()

        # Verify actual row count after load
        cur.execute(f"SELECT COUNT(*) FROM {schema_table}")
        actual_count = cur.fetchone()[0]
        expected_count = EXPECTED_ROW_COUNTS.get(table)

        if expected_count:
            variance = abs(actual_count - expected_count) / expected_count
            if variance > 0.01:
                logger.warning(
                    f"  Row count variance for {table}: "
                    f"expected {expected_count:,}, got {actual_count:,} "
                    f"({variance:.1%} variance)"
                )
            else:
                logger.info(
                    f"  {table}: {actual_count:,} rows loaded "
                    f"(expected {expected_count:,}) OK"
                )
        else:
            logger.info(f"  {table}: {actual_count:,} rows loaded")

    except Exception as exc:
        conn.rollback()
        logger.error(f"Failed to load {table}: {exc}")
        raise
    finally:
        cur.close()


# ============================================================================
# MAIN
# ============================================================================

def run():
    """
    Main loader function.
    1. Connect to PostgreSQL
    2. Check if bronze tables exist, create if not
    3. Load each CSV file into its bronze table
    4. Log summary
    """
    logger.info("=" * 60)
    logger.info("Bronze Layer Loader - Starting")
    logger.info(f"Source directory: {RAW_DATA_DIR}")
    logger.info("=" * 60)

    start_time = datetime.now()

    # Verify raw data directory exists
    if not RAW_DATA_DIR.exists():
        logger.error(f"Raw data directory not found: {RAW_DATA_DIR}")
        sys.exit(1)

    # Verify all CSV files are present
    missing_files = []
    for filename in FILE_TABLE_MAP:
        if not (RAW_DATA_DIR / filename).exists():
            missing_files.append(filename)

    if missing_files:
        logger.error(f"Missing CSV files: {missing_files}")
        logger.error(f"Expected in: {RAW_DATA_DIR}")
        sys.exit(1)

    logger.info("All CSV files found")

    # Connect to database
    try:
        conn = get_connection()
        logger.info("Database connection established")
    except Exception as exc:
        logger.error(f"Database connection failed: {exc}")
        sys.exit(1)

    try:
        # Create bronze schema if tables do not exist
        if not bronze_tables_exist(conn):
            logger.info("Bronze tables not found - running DDL setup")
            create_bronze_schema(conn)
        else:
            logger.info("Bronze tables exist - skipping DDL setup")

        # Load each CSV file
        loaded = []
        failed = []

        for filename, config in FILE_TABLE_MAP.items():
            table = config['table']
            primary_keys = config['primary_keys']
            filepath = RAW_DATA_DIR / filename

            logger.info(f"\nLoading: {filename} -> bronze.{table}")

            try:
                df = read_csv(filepath, table)
                df = deduplicate(df, primary_keys, table)
                df = add_audit_columns(df, filename)
                load_table(conn, df, table, primary_keys)
                loaded.append(table)
            except Exception as exc:
                logger.error(f"Failed to load {table}: {exc}")
                failed.append(table)
                continue

        # Summary
        elapsed = (datetime.now() - start_time).seconds
        logger.info("\n" + "=" * 60)
        logger.info("Bronze Layer Loader - Complete")
        logger.info(f"  Loaded:  {len(loaded)} tables: {loaded}")
        logger.info(f"  Failed:  {len(failed)} tables: {failed}")
        logger.info(f"  Elapsed: {elapsed} seconds")
        logger.info("=" * 60)

        if failed:
            logger.error("Some tables failed to load. Check error.log for details.")
            sys.exit(1)

    finally:
        conn.close()


if __name__ == '__main__':
    run()
