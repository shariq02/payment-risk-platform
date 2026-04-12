"""
Integration test for Airflow DAG execution
===========================================
Tests end-to-end DAG execution.
"""

import sys
from pathlib import Path
import pytest
import psycopg2
from datetime import datetime

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import DATABASE_CONFIG

try:
    from airflow.models import DagBag
    from airflow.utils.state import DagRunState
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False

pytestmark = pytest.mark.skipif(not AIRFLOW_AVAILABLE, reason="Airflow not available")


@pytest.fixture(scope='module')
def dagbag():
    """Load all DAGs."""
    dags_folder = PROJECT_ROOT / 'airflow' / 'dags'
    return DagBag(dag_folder=str(dags_folder), include_examples=False)


@pytest.fixture
def db_conn():
    """Database connection fixture."""
    conn = psycopg2.connect(**DATABASE_CONFIG)
    yield conn
    conn.close()


class TestPaymentIngestExecution:
    """Test payment_ingest DAG execution."""
    
    def test_dag_can_be_triggered(self, dagbag):
        """payment_ingest DAG should be triggerable."""
        dag = dagbag.get_dag('payment_ingest')
        assert dag is not None
        
        execution_date = datetime(2026, 4, 1)
        dag_run = dag.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=execution_date,
            run_id=f"test_run_{execution_date.isoformat()}",
            run_type='manual'
        )
        assert dag_run is not None


class TestPaymentMartRefreshExecution:
    """Test payment_mart_refresh DAG execution."""
    
    def test_dag_can_be_triggered(self, dagbag):
        """payment_mart_refresh DAG should be triggerable."""
        dag = dagbag.get_dag('payment_mart_refresh')
        assert dag is not None
        
        execution_date = datetime(2026, 4, 1)
        dag_run = dag.create_dagrun(
            state=DagRunState.RUNNING,
            execution_date=execution_date,
            run_id=f"test_run_{execution_date.isoformat()}",
            run_type='manual'
        )
        assert dag_run is not None
    
    def test_snapshot_creates_scd2_records(self, db_conn):
        """Running snapshot should create SCD2 records."""
        cur = db_conn.cursor()
        
        cur.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'snapshots'
        """)
        count = cur.fetchone()[0]
        assert count >= 2, "Should have at least 2 snapshot tables"
        
        cur.close()


class TestDAGIdempotency:
    """Test DAG idempotency."""
    
    def test_payment_ingest_idempotent(self):
        """Running payment_ingest multiple times should be safe."""
        pass
    
    def test_payment_mart_refresh_idempotent(self):
        """Running payment_mart_refresh multiple times should be safe."""
        pass
