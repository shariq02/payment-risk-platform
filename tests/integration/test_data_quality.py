"""
Integration test for data quality
==================================
Tests row counts, NULL checks, constraint violations.
"""

import sys
from pathlib import Path
import pytest
import psycopg2

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import DATABASE_CONFIG


@pytest.fixture
def db_conn():
    """Database connection fixture."""
    conn = psycopg2.connect(**DATABASE_CONFIG)
    yield conn
    conn.close()


class TestRowCounts:
    """Test expected row counts in tables."""
    
    def test_bronze_orders_count(self, db_conn):
        """Bronze orders should have expected row count."""
        cur = db_conn.cursor()
        cur.execute("SELECT COUNT(*) FROM bronze.orders")
        count = cur.fetchone()[0]
        assert count == 99441, f"Expected 99441 orders, got {count}"
        cur.close()
    
    def test_bronze_order_items_count(self, db_conn):
        """Bronze order_items should have expected row count."""
        cur = db_conn.cursor()
        cur.execute("SELECT COUNT(*) FROM bronze.order_items")
        count = cur.fetchone()[0]
        assert count == 112650, f"Expected 112650 order items, got {count}"
        cur.close()
    
    def test_fact_payments_count(self, db_conn):
        """fact_order_payments should have 103886 rows."""
        cur = db_conn.cursor()
        cur.execute("SELECT COUNT(*) FROM mart.fact_order_payments")
        count = cur.fetchone()[0]
        assert count == 103886, f"Expected 103886 payments, got {count}"
        cur.close()
    
    def test_fact_fulfillment_count(self, db_conn):
        """fact_order_fulfillment should have 112650 rows."""
        cur = db_conn.cursor()
        cur.execute("SELECT COUNT(*) FROM mart.fact_order_fulfillment")
        count = cur.fetchone()[0]
        assert count == 112650, f"Expected 112650 fulfillment records, got {count}"
        cur.close()


class TestNullChecks:
    """Test NULL handling in critical columns."""
    
    def test_no_null_primary_keys_bronze(self, db_conn):
        """Bronze tables should have no NULL primary keys."""
        cur = db_conn.cursor()
        
        cur.execute("SELECT COUNT(*) FROM bronze.orders WHERE order_id IS NULL")
        assert cur.fetchone()[0] == 0
        
        cur.execute("SELECT COUNT(*) FROM bronze.customers WHERE customer_id IS NULL")
        assert cur.fetchone()[0] == 0
        
        cur.close()
    
    def test_no_null_foreign_keys_facts(self, db_conn):
        """Fact tables should have no NULL foreign keys."""
        cur = db_conn.cursor()
        
        cur.execute("""
            SELECT COUNT(*) 
            FROM mart.fact_order_payments
            WHERE customer_unique_id IS NULL
        """)
        assert cur.fetchone()[0] == 0
        
        cur.execute("""
            SELECT COUNT(*) 
            FROM mart.fact_order_fulfillment
            WHERE seller_id IS NULL
        """)
        assert cur.fetchone()[0] == 0
        
        cur.close()
    
    def test_expected_nulls_in_timestamps(self, db_conn):
        """Some timestamps should have NULLs (not all orders delivered)."""
        cur = db_conn.cursor()
        
        cur.execute("""
            SELECT COUNT(*) 
            FROM bronze.orders
            WHERE order_delivered_customer_date IS NULL
        """)
        null_count = cur.fetchone()[0]
        assert null_count > 0, "Should have NULL delivered dates for undelivered orders"
        
        cur.close()


class TestDataConsistency:
    """Test data consistency across layers."""
    
    def test_staging_count_matches_bronze(self, db_conn):
        """Staging views should have same row counts as bronze."""
        cur = db_conn.cursor()
        
        cur.execute("SELECT COUNT(*) FROM bronze.orders")
        bronze_count = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM staging.stg_orders")
        staging_count = cur.fetchone()[0]
        
        assert bronze_count == staging_count
        
        cur.close()
    
    def test_no_orphaned_facts(self, db_conn):
        """Facts should not have orphaned dimension references."""
        cur = db_conn.cursor()
        
        cur.execute("""
            SELECT COUNT(*) 
            FROM mart.fact_order_payments f
            LEFT JOIN mart.dim_customer d 
                ON f.customer_unique_id = d.customer_unique_id
                AND d.is_current = true
            WHERE d.customer_unique_id IS NULL
        """)
        assert cur.fetchone()[0] == 0, "No orphaned customers in facts"
        
        cur.close()
    
    def test_dimension_uniqueness(self, db_conn):
        """Current dimensions should have unique keys."""
        cur = db_conn.cursor()
        
        cur.execute("""
            SELECT customer_unique_id, COUNT(*) 
            FROM mart.dim_customer
            WHERE is_current = true
            GROUP BY customer_unique_id
            HAVING COUNT(*) > 1
        """)
        duplicates = cur.fetchall()
        assert len(duplicates) == 0, "dim_customer should have unique current records"
        
        cur.close()


class TestRiskScores:
    """Test risk score validity."""
    
    def test_risk_scores_in_range(self, db_conn):
        """Risk scores should be between 0 and 1."""
        cur = db_conn.cursor()
        
        cur.execute("""
            SELECT COUNT(*) 
            FROM mart.fact_order_payments
            WHERE payment_risk_score < 0 OR payment_risk_score > 1
        """)
        assert cur.fetchone()[0] == 0
        
        cur.execute("""
            SELECT COUNT(*) 
            FROM mart.fact_order_fulfillment
            WHERE fulfillment_risk_score < 0 OR fulfillment_risk_score > 1
        """)
        assert cur.fetchone()[0] == 0
        
        cur.close()
    
    def test_risk_scores_not_all_same(self, db_conn):
        """Risk scores should vary across records."""
        cur = db_conn.cursor()
        
        cur.execute("""
            SELECT COUNT(DISTINCT payment_risk_score) 
            FROM mart.fact_order_payments
        """)
        distinct_scores = cur.fetchone()[0]
        assert distinct_scores > 1, "Risk scores should vary"
        
        cur.close()
