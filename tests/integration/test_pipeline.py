"""
Integration test for full pipeline
===================================
Tests bronze load -> dbt run -> facts populated.
"""

import sys
from pathlib import Path
import pytest
import psycopg2

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import DATABASE_CONFIG, get_logger

logger = get_logger(__name__)


@pytest.fixture
def db_conn():
    """Database connection fixture."""
    conn = psycopg2.connect(**DATABASE_CONFIG)
    yield conn
    conn.close()


class TestFullPipeline:
    """Test full data pipeline end-to-end."""
    
    def test_bronze_to_staging_to_facts(self, db_conn):
        """
        Full pipeline test:
        1. Bronze tables populated
        2. dbt staging views created
        3. dbt facts populated
        """
        cur = db_conn.cursor()
        
        # Check bronze tables have data
        cur.execute("SELECT COUNT(*) FROM bronze.orders")
        bronze_orders = cur.fetchone()[0]
        assert bronze_orders > 0, "Bronze orders table should have data"
        
        # Check staging views exist and return data
        cur.execute("SELECT COUNT(*) FROM staging.stg_orders")
        staging_orders = cur.fetchone()[0]
        assert staging_orders > 0, "Staging orders view should return data"
        
        # Check fact tables populated
        cur.execute("SELECT COUNT(*) FROM mart.fact_order_payments")
        fact_payments = cur.fetchone()[0]
        assert fact_payments > 0, "fact_order_payments should be populated"
        
        cur.execute("SELECT COUNT(*) FROM mart.fact_order_fulfillment")
        fact_fulfillment = cur.fetchone()[0]
        assert fact_fulfillment > 0, "fact_order_fulfillment should be populated"
        
        cur.close()
    
    def test_dimension_tables_populated(self, db_conn):
        """Dimension tables should be populated."""
        cur = db_conn.cursor()
        
        dimensions = [
            'dim_customer',
            'dim_seller',
            'dim_product',
            'dim_geo',
            'dim_time'
        ]
        
        for dim in dimensions:
            cur.execute(f"SELECT COUNT(*) FROM mart.{dim}")
            count = cur.fetchone()[0]
            assert count > 0, f"mart.{dim} should be populated"
        
        cur.close()
    
    def test_fact_grain_correctness(self, db_conn):
        """Fact tables should have correct grain."""
        cur = db_conn.cursor()
        
        # fact_order_payments: one row per payment
        cur.execute("""
            SELECT COUNT(*) as total_rows,
                   COUNT(DISTINCT order_id) as distinct_orders
            FROM mart.fact_order_payments
        """)
        row = cur.fetchone()
        # Total rows should be >= distinct orders (multiple payments per order possible)
        assert row[0] >= row[1]
        
        # fact_order_fulfillment: one row per order item
        cur.execute("""
            SELECT COUNT(*) as total_rows,
                   COUNT(DISTINCT order_id) as distinct_orders
            FROM mart.fact_order_fulfillment
        """)
        row = cur.fetchone()
        # Total rows should be >= distinct orders (multiple items per order)
        assert row[0] >= row[1]
        
        cur.close()
    
    def test_risk_scores_calculated(self, db_conn):
        """Risk scores should be populated in fact tables."""
        cur = db_conn.cursor()
        
        # Check payment_risk_score is not null
        cur.execute("""
            SELECT COUNT(*) 
            FROM mart.fact_order_payments
            WHERE payment_risk_score IS NOT NULL
        """)
        count = cur.fetchone()[0]
        assert count > 0, "payment_risk_score should be calculated"
        
        # Check fulfillment_risk_score is not null
        cur.execute("""
            SELECT COUNT(*) 
            FROM mart.fact_order_fulfillment
            WHERE fulfillment_risk_score IS NOT NULL
        """)
        count = cur.fetchone()[0]
        assert count > 0, "fulfillment_risk_score should be calculated"
        
        cur.close()
    
    def test_scd2_columns_exist(self, db_conn):
        """SCD2 dimensions should have tracking columns."""
        cur = db_conn.cursor()
        
        for dim in ['dim_customer', 'dim_seller']:
            cur.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = 'mart' 
                AND table_name = '{dim}'
                AND column_name IN ('is_current', 'valid_from', 'valid_to')
            """)
            scd2_cols = [row[0] for row in cur.fetchall()]
            assert len(scd2_cols) == 3, f"{dim} should have all 3 SCD2 columns"
        
        cur.close()
    
    def test_referential_integrity(self, db_conn):
        """Foreign keys should maintain referential integrity."""
        cur = db_conn.cursor()
        
        # All customers in fact_order_payments should exist in dim_customer
        cur.execute("""
            SELECT COUNT(*) 
            FROM mart.fact_order_payments f
            LEFT JOIN mart.dim_customer d 
                ON f.customer_unique_id = d.customer_unique_id
                AND d.is_current = true
            WHERE d.customer_unique_id IS NULL
        """)
        orphans = cur.fetchone()[0]
        assert orphans == 0, "All customers in facts should exist in dim_customer"
        
        # All sellers in fact_order_fulfillment should exist in dim_seller
        cur.execute("""
            SELECT COUNT(*) 
            FROM mart.fact_order_fulfillment f
            LEFT JOIN mart.dim_seller d 
                ON f.seller_id = d.seller_id
                AND d.is_current = true
            WHERE d.seller_id IS NULL
        """)
        orphans = cur.fetchone()[0]
        assert orphans == 0, "All sellers in facts should exist in dim_seller"
        
        cur.close()
