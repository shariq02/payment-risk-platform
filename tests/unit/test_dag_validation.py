"""
Unit tests for Airflow DAG validation
======================================
Tests DAG structure, dependencies, and schedules.
"""

import sys
from pathlib import Path
import pytest

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

try:
    from airflow.models import DagBag
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

pytestmark = pytest.mark.skipif(not AIRFLOW_AVAILABLE, reason="Airflow not available")


@pytest.fixture(scope='module')
def dagbag():
    """Load all DAGs from airflow/dags directory."""
    dags_folder = PROJECT_ROOT / 'airflow' / 'dags'
    return DagBag(dag_folder=str(dags_folder), include_examples=False)


class TestDAGIntegrity:
    """Test DAG loading and structure."""
    
    def test_dags_load_without_errors(self, dagbag):
        """All DAGs should load without import errors."""
        assert len(dagbag.import_errors) == 0, f"DAG import errors: {dagbag.import_errors}"
    
    def test_expected_dags_present(self, dagbag):
        """Both expected DAGs should be present."""
        expected_dags = ['payment_ingest', 'payment_mart_refresh']
        dag_ids = list(dagbag.dag_ids)
        
        for dag_id in expected_dags:
            assert dag_id in dag_ids, f"DAG {dag_id} not found"


class TestPaymentIngestDAG:
    """Test payment_ingest DAG configuration."""
    
    def test_dag_exists(self, dagbag):
        """payment_ingest DAG should exist."""
        assert 'payment_ingest' in dagbag.dags
    
    def test_dag_schedule(self, dagbag):
        """payment_ingest should run every 30 minutes."""
        dag = dagbag.get_dag('payment_ingest')
        assert dag.schedule_interval == '*/30 * * * *'
    
    def test_dag_has_tasks(self, dagbag):
        """payment_ingest should have expected tasks."""
        dag = dagbag.get_dag('payment_ingest')
        task_ids = [task.task_id for task in dag.tasks]
        
        expected_tasks = [
            'check_new_data',
            'load_bronze',
            'quality_gate',
            'notify_success'
        ]
        
        for task_id in expected_tasks:
            assert task_id in task_ids, f"Task {task_id} not found"
    
    def test_dag_task_dependencies(self, dagbag):
        """payment_ingest tasks should have correct dependencies."""
        dag = dagbag.get_dag('payment_ingest')
        
        check_new_data = dag.get_task('check_new_data')
        load_bronze = dag.get_task('load_bronze')
        
        assert check_new_data in load_bronze.upstream_list


class TestPaymentMartRefreshDAG:
    """Test payment_mart_refresh DAG configuration."""
    
    def test_dag_exists(self, dagbag):
        """payment_mart_refresh DAG should exist."""
        assert 'payment_mart_refresh' in dagbag.dags
    
    def test_dag_schedule(self, dagbag):
        """payment_mart_refresh should run every 30 minutes."""
        dag = dagbag.get_dag('payment_mart_refresh')
        assert dag.schedule_interval == '*/30 * * * *'
    
    def test_dag_has_tasks(self, dagbag):
        """payment_mart_refresh should have expected tasks."""
        dag = dagbag.get_dag('payment_mart_refresh')
        task_ids = [task.task_id for task in dag.tasks]
        
        expected_tasks = [
            'check_snapshot_time',
            'run_snapshots',
            'skip_snapshots',
            'run_staging',
            'run_dimensions',
            'run_facts',
            'test_marts',
            'notify_success'
        ]
        
        for task_id in expected_tasks:
            assert task_id in task_ids, f"Task {task_id} not found"
    
    def test_dag_has_branch_operator(self, dagbag):
        """payment_mart_refresh should use BranchPythonOperator."""
        dag = dagbag.get_dag('payment_mart_refresh')
        branch_task = dag.get_task('check_snapshot_time')
        
        from airflow.operators.python import BranchPythonOperator
        assert isinstance(branch_task, BranchPythonOperator)
    
    def test_dag_task_dependencies(self, dagbag):
        """payment_mart_refresh tasks should have correct dependencies."""
        dag = dagbag.get_dag('payment_mart_refresh')
        
        run_dimensions = dag.get_task('run_dimensions')
        run_facts = dag.get_task('run_facts')
        
        assert run_dimensions in run_facts.upstream_list


class TestDAGDefaultArgs:
    """Test DAG default arguments."""
    
    def test_payment_ingest_default_args(self, dagbag):
        """payment_ingest should have proper default args."""
        dag = dagbag.get_dag('payment_ingest')
        
        assert dag.default_args.get('owner') is not None
        assert dag.default_args.get('retries') >= 0
        assert dag.default_args.get('retry_delay') is not None
    
    def test_payment_mart_refresh_default_args(self, dagbag):
        """payment_mart_refresh should have proper default args."""
        dag = dagbag.get_dag('payment_mart_refresh')
        
        assert dag.default_args.get('owner') is not None
        assert dag.default_args.get('retries') >= 0
        assert dag.default_args.get('retry_delay') is not None
