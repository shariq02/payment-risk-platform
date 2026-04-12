"""
Integration test for dbt snapshots
===================================
Tests SCD2 snapshot behavior.
"""

import sys
from pathlib import Path
import pytest
import psycopg2
from datetime import datetime, timedelta

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import DATABASE_CONFIG


@pytest.fixture
def db_conn():
    """Database connection fixture."""
    conn = psycopg2.connect(**DATABASE_CONFIG)
    yield conn
    conn.close()


class TestSnapshotTables:
    """Test snapshot table existence and structure."""
    
    def test_customer_snapshot_exists(self, db_conn):
        """snap_customer_profile should exist in snapshots schema."""
        cur = db_conn.cursor()
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'snapshots' 
            AND table_name = 'snap_customer_profile'
        """)
        result = cur.fetchone()
        assert result is not None
        cur.close()
    
    def test_seller_snapshot_exists(self, db_conn):
        """snap_seller_risk_profile should exist in snapshots schema."""
        cur = db_conn.cursor()
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'snapshots' 
            AND table_name = 'snap_seller_risk_profile'
        """)
        result = cur.fetchone()
        assert result is not None
        cur.close()
    
    def test_snapshot_has_scd2_columns(self, db_conn):
        """Snapshot tables should have dbt SCD2 columns."""
        cur = db_conn.cursor()
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'snapshots' 
            AND table_name = 'snap_customer_profile'
            AND column_name IN ('dbt_scd_id', 'dbt_updated_at', 'dbt_valid_from', 'dbt_valid_to')
        """)
        scd2_cols = [row[0] for row in cur.fetchall()]
        assert len(scd2_cols) == 4
        cur.close()


class TestSnapshotBehavior:
    """Test SCD2 tracking behavior."""
    
    def test_snapshot_has_rows(self, db_conn):
        """Snapshot tables should have data."""
        cur = db_conn.cursor()
        
        cur.execute("SELECT COUNT(*) FROM snapshots.snap_customer_profile")
        customer_count = cur.fetchone()[0]
        assert customer_count > 0
        
        cur.execute("SELECT COUNT(*) FROM snapshots.snap_seller_risk_profile")
        seller_count = cur.fetchone()[0]
        assert seller_count > 0
        
        cur.close()
    
    def test_snapshot_tracks_changes(self, db_conn):
        """Snapshots should have dbt_valid_to NULL for current records."""
        cur = db_conn.cursor()
        
        cur.execute("""
            SELECT COUNT(*) 
            FROM snapshots.snap_customer_profile
            WHERE dbt_valid_to IS NULL
        """)
        current_records = cur.fetchone()[0]
        assert current_records > 0, "Should have current records with NULL dbt_valid_to"
        
        cur.close()
    
    def test_snapshot_unique_scd_ids(self, db_conn):
        """Each snapshot row should have unique dbt_scd_id."""
        cur = db_conn.cursor()
        
        cur.execute("""
            SELECT COUNT(DISTINCT dbt_scd_id) as unique_ids,
                   COUNT(*) as total_rows
            FROM snapshots.snap_customer_profile
        """)
        row = cur.fetchone()
        assert row[0] == row[1], "dbt_scd_id should be unique"
        
        cur.close()


class TestSnapshotPointInTime:
    """Test point-in-time queries on snapshots."""
    
    def test_point_in_time_query_works(self, db_conn):
        """Should be able to query snapshot state at a point in time."""
        cur = db_conn.cursor()
        
        cur.execute("""
            SELECT COUNT(*) 
            FROM snapshots.snap_customer_profile
            WHERE dbt_valid_from <= CURRENT_TIMESTAMP
            AND (dbt_valid_to IS NULL OR dbt_valid_to > CURRENT_TIMESTAMP)
        """)
        current_state = cur.fetchone()[0]
        assert current_state > 0
        
        cur.close()
    
    def test_snapshot_covers_all_current_dimensions(self, db_conn):
        """Current snapshot should cover all current dimension records."""
        cur = db_conn.cursor()
        
        cur.execute("""
            SELECT COUNT(DISTINCT customer_unique_id) 
            FROM mart.dim_customer
            WHERE is_current = true
        """)
        dim_count = cur.fetchone()[0]
        
        cur.execute("""
            SELECT COUNT(DISTINCT customer_unique_id) 
            FROM snapshots.snap_customer_profile
            WHERE dbt_valid_to IS NULL
        """)
        snapshot_count = cur.fetchone()[0]
        
        assert snapshot_count == dim_count, "Snapshot should match current dim records"
        
        cur.close()
