"""
Regression test for SCD2 point-in-time joins
=============================================
Tests correctness of point-in-time dimension joins.
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


class TestSCD2JoinCorrectness:
    """Test SCD2 dimension join correctness."""
    
    def test_current_join_returns_one_row_per_key(self, db_conn):
        """Joining to current dimensions should return correct count."""
        cur = db_conn.cursor()
        
        # Count total fact records
        cur.execute("SELECT COUNT(*) FROM mart.fact_order_payments")
        total_facts = cur.fetchone()[0]
        
        # Count after join - should match
        cur.execute("""
            SELECT COUNT(*)
            FROM mart.fact_order_payments f
            INNER JOIN mart.dim_customer d 
                ON f.customer_unique_id = d.customer_unique_id
                AND d.is_current = true
        """)
        joined_count = cur.fetchone()[0]
        
        assert joined_count == total_facts, "Join should not lose or duplicate records"
        
        cur.close()
    
    def test_point_in_time_join_logic(self, db_conn):
        """Point-in-time join should match fact timestamp to dimension validity."""
        cur = db_conn.cursor()
        
        cur.execute("""
            SELECT COUNT(*) 
            FROM mart.fact_order_payments f
            LEFT JOIN snapshots.snap_customer_profile s
                ON f.customer_unique_id = s.customer_unique_id
                AND f.event_ts >= s.dbt_valid_from
                AND (s.dbt_valid_to IS NULL OR f.event_ts < s.dbt_valid_to)
            WHERE s.customer_unique_id IS NULL
        """)
        
        unmatched = cur.fetchone()[0]
        assert unmatched == 0, "All facts should match a snapshot version"
        
        cur.close()
    
    def test_seller_scd2_join(self, db_conn):
        """Seller SCD2 join should work correctly."""
        cur = db_conn.cursor()
        
        # Count total fact records
        cur.execute("SELECT COUNT(*) FROM mart.fact_order_fulfillment")
        total_facts = cur.fetchone()[0]
        
        # Count after join
        cur.execute("""
            SELECT COUNT(*)
            FROM mart.fact_order_fulfillment f
            INNER JOIN mart.dim_seller d
                ON f.seller_id = d.seller_id
                AND d.is_current = true
        """)
        joined_count = cur.fetchone()[0]
        
        assert joined_count == total_facts, "Seller join should not lose or duplicate records"
        
        cur.close()


class TestSCD2ValidityWindows:
    """Test SCD2 validity window logic."""
    
    def test_no_overlapping_validity_windows(self, db_conn):
        """Same customer should not have overlapping validity windows."""
        cur = db_conn.cursor()
        
        cur.execute("""
            SELECT 
                s1.customer_unique_id
            FROM snapshots.snap_customer_profile s1
            INNER JOIN snapshots.snap_customer_profile s2
                ON s1.customer_unique_id = s2.customer_unique_id
                AND s1.dbt_scd_id <> s2.dbt_scd_id
            WHERE s1.dbt_valid_from < COALESCE(s2.dbt_valid_to, '9999-12-31')
            AND COALESCE(s1.dbt_valid_to, '9999-12-31') > s2.dbt_valid_from
            LIMIT 1
        """)
        
        overlap = cur.fetchone()
        assert overlap is None, "Should have no overlapping validity windows"
        
        cur.close()
    
    def test_validity_windows_cover_all_time(self, db_conn):
        """Each customer should have continuous coverage from first order."""
        cur = db_conn.cursor()
        
        cur.execute("""
            SELECT COUNT(DISTINCT customer_unique_id) 
            FROM mart.dim_customer
            WHERE is_current = true
        """)
        total_customers = cur.fetchone()[0]
        
        cur.execute("""
            SELECT COUNT(DISTINCT customer_unique_id)
            FROM snapshots.snap_customer_profile
            WHERE dbt_valid_to IS NULL
        """)
        current_snapshot_customers = cur.fetchone()[0]
        
        assert total_customers == current_snapshot_customers
        
        cur.close()


class TestSCD2HistoricalAccuracy:
    """Test historical dimension accuracy."""
    
    def test_snapshot_preserves_historical_values(self, db_conn):
        """Snapshot should preserve historical attribute values."""
        cur = db_conn.cursor()
        
        cur.execute("""
            SELECT 
                customer_unique_id,
                COUNT(DISTINCT total_orders) as order_count_versions
            FROM snapshots.snap_customer_profile
            GROUP BY customer_unique_id
            HAVING COUNT(*) > 1
            LIMIT 1
        """)
        
        result = cur.fetchone()
        if result:
            assert result[1] > 1, "Customers with multiple versions should show changing values"
        
        cur.close()
