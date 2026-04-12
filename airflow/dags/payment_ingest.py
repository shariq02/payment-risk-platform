"""
Payment Risk Platform - Bronze Ingestion DAG
=============================================
Loads raw Olist CSV files into PostgreSQL bronze schema every 15 minutes.

Schedule: Every 15 minutes
Owner: Data Platform Team
Start Date: 2026-04-01

Tasks:
  1. check_new_data: ShortCircuitOperator - skip if no new files detected
  2. load_bronze: Run load_bronze.py to ingest CSVs
  3. quality_gate: Verify row counts and data quality
  4. notify_success: Log successful completion

Quality Gate Checks:
  - Row count within 1% of expected
  - No duplicate primary keys
  - All audit columns populated
  - Payment types are valid
  - Review scores in range 1-5

Failure Handling:
  - Email alert on quality gate failure
  - DAG stops at quality gate - does not proceed to mart refresh
"""

from datetime import datetime, timedelta
from pathlib import Path
import logging
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

import psycopg2

# Import project config
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config import (
    PROJECT_ROOT,
    RAW_DATA_DIR,
    LOADER_DIR,
    DATABASE_CONFIG,
)

# ============================================================================
# CONFIGURATION
# ============================================================================

BRONZE_LOADER_SCRIPT = LOADER_DIR / 'load_bronze.py'
DB_CONFIG = DATABASE_CONFIG

EXPECTED_ROW_COUNTS = {
    'orders': 99441,
    'order_items': 112650,
    'order_payments': 103886,
    'customers': 99441,
    'sellers': 3095,
    'products': 32951,
    'reviews': 99224,
    'geolocation': 1000163,
}

# CSV files to monitor for changes
REQUIRED_CSV_FILES = [
    'olist_orders_dataset.csv',
    'olist_order_items_dataset.csv',
    'olist_order_payments_dataset.csv',
    'olist_customers_dataset.csv',
    'olist_sellers_dataset.csv',
    'olist_products_dataset.csv',
    'olist_order_reviews_dataset.csv',
    'olist_geolocation_dataset.csv',
]

# ============================================================================
# TASK FUNCTIONS
# ============================================================================

def check_new_data(**context):
    """
    Check if new data files exist or have been modified since last run.
    Returns True if new data detected, False otherwise.
    ShortCircuitOperator will skip downstream tasks if False.
    """
    logger = logging.getLogger(__name__)
    
    # Check if all required CSV files exist
    missing_files = []
    for filename in REQUIRED_CSV_FILES:
        filepath = RAW_DATA_DIR / filename
        if not filepath.exists():
            missing_files.append(filename)
    
    if missing_files:
        logger.warning(f"Missing CSV files: {missing_files}")
        logger.info("Skipping ingestion - no data to load")
        return False
    
    # For initial implementation, always return True
    # Future enhancement: track last_modified timestamps in XCom or metadata table
    logger.info("All CSV files present - proceeding with ingestion")
    return True


def run_bronze_loader(**context):
    """Execute load_bronze.py script to load CSVs into bronze tables."""
    logger = logging.getLogger(__name__)
    logger.info(f"Executing bronze loader: {BRONZE_LOADER_SCRIPT}")
    
    import subprocess
    result = subprocess.run(
        ['python3.11', str(BRONZE_LOADER_SCRIPT)],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
    )
    
    if result.returncode != 0:
        logger.error(f"Bronze loader failed:\n{result.stderr}")
        raise RuntimeError(f"Bronze loader failed with code {result.returncode}")
    
    logger.info("Bronze loader completed successfully")
    logger.info(result.stdout)


def quality_gate(**context):
    """
    Verify bronze data quality after ingestion.
    Raises exception if any check fails - stops DAG execution.
    """
    logger = logging.getLogger(__name__)
    logger.info("Running quality gate checks")
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    failed_checks = []
    
    try:
        # Check 1: Row counts within 1% of expected
        logger.info("Check 1: Row counts")
        for table, expected in EXPECTED_ROW_COUNTS.items():
            cur.execute(f"SELECT COUNT(*) FROM bronze.{table}")
            actual = cur.fetchone()[0]
            variance = abs(actual - expected) / expected
            
            if variance > 0.01:
                failed_checks.append(
                    f"Row count variance for {table}: expected {expected:,}, "
                    f"got {actual:,} ({variance:.1%} variance)"
                )
            else:
                logger.info(f"  {table}: {actual:,} rows OK")
        
        # Check 2: No duplicate primary keys
        logger.info("Check 2: Primary key duplicates")
        
        # Single PK tables
        single_pk_checks = {
            'orders': 'order_id',
            'customers': 'customer_id',
            'sellers': 'seller_id',
            'products': 'product_id',
        }
        for table, pk in single_pk_checks.items():
            cur.execute(f"""
                SELECT COUNT(*) - COUNT(DISTINCT {pk})
                FROM bronze.{table}
            """)
            duplicates = cur.fetchone()[0]
            if duplicates > 0:
                failed_checks.append(f"{table} has {duplicates} duplicate {pk} values")
            else:
                logger.info(f"  {table}: no duplicates")
        
        # Composite PK tables
        composite_pk_checks = {
            'order_items': ['order_id', 'order_item_id'],
            'order_payments': ['order_id', 'payment_sequential'],
            'reviews': ['review_id', 'order_id'],
        }
        for table, pks in composite_pk_checks.items():
            pk_cols = ', '.join(pks)
            cur.execute(f"""
                SELECT COUNT(*) - COUNT(DISTINCT ({pk_cols}))
                FROM bronze.{table}
            """)
            duplicates = cur.fetchone()[0]
            if duplicates > 0:
                failed_checks.append(f"{table} has {duplicates} duplicate composite key values")
            else:
                logger.info(f"  {table}: no duplicates")
        
        # Check 3: Audit columns populated
        logger.info("Check 3: Audit columns")
        for table in EXPECTED_ROW_COUNTS.keys():
            cur.execute(f"""
                SELECT COUNT(*)
                FROM bronze.{table}
                WHERE _ingested_at IS NULL
            """)
            null_count = cur.fetchone()[0]
            if null_count > 0:
                failed_checks.append(f"{table} has {null_count} rows with NULL _ingested_at")
            else:
                logger.info(f"  {table}: _ingested_at populated")
        
        # Check 4: Payment types valid
        logger.info("Check 4: Payment types")
        cur.execute("""
            SELECT DISTINCT payment_type
            FROM bronze.order_payments
            ORDER BY payment_type
        """)
        actual_types = {row[0] for row in cur.fetchall()}
        expected_types = {'boleto', 'credit_card', 'debit_card', 'not_defined', 'voucher'}
        
        if actual_types != expected_types:
            failed_checks.append(
                f"Invalid payment types: expected {expected_types}, got {actual_types}"
            )
        else:
            logger.info(f"  Payment types valid: {len(actual_types)} types")
        
        # Check 5: Review scores in range
        logger.info("Check 5: Review score range")
        cur.execute("""
            SELECT COUNT(*)
            FROM bronze.reviews
            WHERE review_score < 1 OR review_score > 5
        """)
        invalid = cur.fetchone()[0]
        if invalid > 0:
            failed_checks.append(f"{invalid} reviews have scores outside 1-5 range")
        else:
            logger.info("  Review scores valid (1-5)")
        
    finally:
        cur.close()
        conn.close()
    
    # Report results
    if failed_checks:
        logger.error("Quality gate FAILED:")
        for check in failed_checks:
            logger.error(f"  - {check}")
        raise RuntimeError(f"Quality gate failed: {len(failed_checks)} checks failed")
    
    logger.info("Quality gate PASSED - all checks successful")


def notify_success(**context):
    """Log successful completion."""
    logger = logging.getLogger(__name__)
    execution_date = context['execution_date']
    logger.info(f"Bronze ingestion completed successfully at {execution_date}")
    logger.info("Ready for mart refresh")


# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = {
    'owner': 'data_platform',
    'depends_on_past': False,
    'email': ['sharique@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='payment_ingest',
    default_args=default_args,
    description='Load Olist CSV files into bronze schema with quality checks',
    schedule_interval='*/15 * * * *',  # Every 15 minutes
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=['bronze', 'ingestion', 'payment_risk'],
) as dag:
    
    check_data = ShortCircuitOperator(
        task_id='check_new_data',
        python_callable=check_new_data,
        provide_context=True,
    )
    
    load_bronze = PythonOperator(
        task_id='load_bronze',
        python_callable=run_bronze_loader,
        provide_context=True,
    )
    
    quality_check = PythonOperator(
        task_id='quality_gate',
        python_callable=quality_gate,
        provide_context=True,
    )
    
    notify = PythonOperator(
        task_id='notify_success',
        python_callable=notify_success,
        provide_context=True,
    )
    
    # Task dependencies
    check_data >> load_bronze >> quality_check >> notify
