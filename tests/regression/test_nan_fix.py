"""
Regression test for NaN bug fix
================================
Ensures NaN bug stays fixed.
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


class TestNaNBugFixed:
    """Test that NaN values are stored as NULL, not string 'NaN'."""
    
    def test_no_nan_strings_in_timestamps(self, db_conn):
        """Timestamp columns should have NULL not 'NaN' string."""
        cur = db_conn.cursor()
        
        cur.execute("""
            SELECT COUNT(*) 
            FROM bronze.orders
            WHERE order_approved_at = 'NaN'
        """)
        assert cur.fetchone()[0] == 0, "Should have no 'NaN' strings in order_approved_at"
        
        cur.execute("""
            SELECT COUNT(*) 
            FROM bronze.orders
            WHERE order_delivered_carrier_date = 'NaN'
        """)
        assert cur.fetchone()[0] == 0, "Should have no 'NaN' strings in order_delivered_carrier_date"
        
        cur.execute("""
            SELECT COUNT(*) 
            FROM bronze.orders
            WHERE order_delivered_customer_date = 'NaN'
        """)
        assert cur.fetchone()[0] == 0, "Should have no 'NaN' strings in order_delivered_customer_date"
        
        cur.close()
    
    def test_no_nan_strings_in_product_numerics(self, db_conn):
        """Product numeric columns should have NULL not 'NaN' string."""
        cur = db_conn.cursor()
        
        numeric_cols = [
            'product_name_lenght',
            'product_description_lenght',
            'product_photos_qty',
            'product_weight_g',
            'product_length_cm',
            'product_height_cm',
            'product_width_cm'
        ]
        
        for col in numeric_cols:
            cur.execute(f"""
                SELECT COUNT(*) 
                FROM bronze.products
                WHERE {col} = 'NaN'
            """)
            count = cur.fetchone()[0]
            assert count == 0, f"Should have no 'NaN' strings in {col}"
        
        cur.close()
    
    def test_proper_null_counts(self, db_conn):
        """Verify expected NULL counts match original issue description."""
        cur = db_conn.cursor()
        
        cur.execute("""
            SELECT COUNT(*) 
            FROM bronze.orders
            WHERE order_approved_at IS NULL
        """)
        assert cur.fetchone()[0] == 160, "Should have 160 NULL order_approved_at"
        
        cur.execute("""
            SELECT COUNT(*) 
            FROM bronze.orders
            WHERE order_delivered_carrier_date IS NULL
        """)
        assert cur.fetchone()[0] == 1783, "Should have 1783 NULL order_delivered_carrier_date"
        
        cur.execute("""
            SELECT COUNT(*) 
            FROM bronze.orders
            WHERE order_delivered_customer_date IS NULL
        """)
        assert cur.fetchone()[0] == 2965, "Should have 2965 NULL order_delivered_customer_date"
        
        cur.close()
    
    def test_staging_handles_nulls_correctly(self, db_conn):
        """Staging layer should handle NULLs with NULLIF guards."""
        cur = db_conn.cursor()
        
        cur.execute("""
            SELECT COUNT(*) 
            FROM staging.stg_orders
            WHERE order_approved_ts IS NULL
        """)
        staging_nulls = cur.fetchone()[0]
        
        cur.execute("""
            SELECT COUNT(*) 
            FROM bronze.orders
            WHERE order_approved_at IS NULL
        """)
        bronze_nulls = cur.fetchone()[0]
        
        assert staging_nulls == bronze_nulls, "Staging should preserve NULL counts"
        
        cur.close()
