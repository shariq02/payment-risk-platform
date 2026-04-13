"""
Payment Risk Platform - Mart Refresh DAG
=========================================
Refreshes dbt staging and mart models every 30 minutes.
Runs snapshots daily at 02:00 AM to capture SCD2 history.

Schedule: Every 30 minutes
Owner: Data Platform Team
Start Date: 2026-04-01

Tasks:
  1. check_snapshot_time: BranchPythonOperator - snapshots only at 02:00
  2a. run_snapshots: dbt snapshot (if 02:00)
  2b. skip_snapshots: dummy task (all other times)
  3. run_staging: dbt run --select staging
  4. run_dimensions: dbt run --select marts.dimensions
  5. run_facts: dbt run --select marts.facts
  6. test_marts: dbt test --select marts
  7. notify_success: Log completion

Branching Logic:
  02:00-02:29: check_snapshot_time -> run_snapshots -> run_staging -> ...
  All other:   check_snapshot_time -> skip_snapshots -> run_staging -> ...

Dependencies:
  - Requires payment_ingest DAG to have completed successfully
  - Bronze tables must be loaded before mart refresh
"""

from datetime import datetime, timedelta
from pathlib import Path
import logging
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

# Import project config
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config import (
    DBT_DIR,
    get_dbt_run_command,
    get_dbt_test_command,
    get_dbt_snapshot_command,
)

# ============================================================================
# CONFIGURATION
# ============================================================================

# Snapshot execution window (02:00-02:29 AM)
SNAPSHOT_HOUR = 2
SNAPSHOT_MINUTE_START = 0
SNAPSHOT_MINUTE_END = 29

# ============================================================================
# TASK FUNCTIONS
# ============================================================================

def check_snapshot_time(**context):
    """
    Determine if current execution time is within snapshot window.
    Returns task_id to execute next based on time.
    
    Snapshot window: 02:00-02:29 daily
    Outside window: skip snapshots, proceed to staging
    """
    logger = logging.getLogger(__name__)
    execution_date = context['execution_date']
    
    hour = execution_date.hour
    minute = execution_date.minute
    
    # Check if within snapshot window
    is_snapshot_time = (
        hour == SNAPSHOT_HOUR and
        SNAPSHOT_MINUTE_START <= minute <= SNAPSHOT_MINUTE_END
    )
    
    if is_snapshot_time:
        logger.info(
            f"Execution at {hour:02d}:{minute:02d} - RUNNING snapshots"
        )
        return 'run_snapshots'
    else:
        logger.info(
            f"Execution at {hour:02d}:{minute:02d} - SKIPPING snapshots "
            f"(only run at {SNAPSHOT_HOUR:02d}:00-{SNAPSHOT_HOUR:02d}:29)"
        )
        return 'skip_snapshots'


def notify_success(**context):
    """Log successful mart refresh completion."""
    logger = logging.getLogger(__name__)
    execution_date = context['execution_date']
    logger.info(f"Mart refresh completed successfully at {execution_date}")
    
    # Log which path was taken
    task_instance = context['task_instance']
    snapshots_ran = task_instance.xcom_pull(task_ids='check_snapshot_time') == 'run_snapshots'
    
    if snapshots_ran:
        logger.info("  Snapshots executed: YES (SCD2 history captured)")
    else:
        logger.info("  Snapshots executed: NO (outside 02:00-02:29 window)")


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
    dag_id='payment_mart_refresh',
    default_args=default_args,
    description='Refresh dbt staging and mart models with conditional snapshots',
    schedule_interval='*/30 * * * *',  # Every 30 minutes
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=['mart', 'dbt', 'payment_risk'],
) as dag:
    
    # Branch based on execution time
    check_time = BranchPythonOperator(
        task_id='check_snapshot_time',
        python_callable=check_snapshot_time,
        provide_context=True,
    )
    
    # Snapshot path (02:00-02:29 only)
    run_snapshots = BashOperator(
        task_id='run_snapshots',
        bash_command=f'cd {DBT_DIR} && {get_dbt_snapshot_command()}',
    )
    
    # Skip path (all other times)
    skip_snapshots = DummyOperator(
        task_id='skip_snapshots',
    )
    
    # Common mart refresh tasks (run after either branch)
    run_staging = BashOperator(
        task_id='run_staging',
        bash_command=f'cd {DBT_DIR} && {get_dbt_run_command(select="staging")}',
        trigger_rule='none_failed_min_one_success',  # Run if either branch succeeded
    )
    
    run_dimensions = BashOperator(
        task_id='run_dimensions',
        bash_command=f'cd {DBT_DIR} && {get_dbt_run_command(select="marts.dimensions")}',
    )
    
    run_facts = BashOperator(
        task_id='run_facts',
        bash_command=f'cd {DBT_DIR} && {get_dbt_run_command(select="marts.facts")}',
    )
    
    test_marts = BashOperator(
        task_id='test_marts',
        bash_command=f'cd {DBT_DIR} && {get_dbt_test_command(select="marts")}',
    )
    
    notify = PythonOperator(
        task_id='notify_success',
        python_callable=notify_success,
        provide_context=True,
    )
    
    # Task dependencies
    # Branch: check_time decides snapshot or skip
    check_time >> [run_snapshots, skip_snapshots]
    
    # Both branches converge at run_staging
    [run_snapshots, skip_snapshots] >> run_staging
    
    # Linear progression through mart layers
    run_staging >> run_dimensions >> run_facts >> test_marts >> notify
