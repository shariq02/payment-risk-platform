"""
Unit tests for setup_warehouse.py
==================================
Tests DDL execution and schema creation.
"""

import sys
from pathlib import Path
import pytest
import psycopg2

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import DATABASE_CONFIG, SQL_DDL_DIR


@pytest.fixture
def db_conn():
    """Database connection fixture."""
    conn = psycopg2.connect(**DATABASE_CONFIG)
    yield conn
    conn.close()


class TestSchemaCreation:
    """Test database schema setup."""
    
    def test_bronze_schema_exists(self, db_conn):
        """Bronze schema should exist."""
        cur = db_conn.cursor()
        cur.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name = 'bronze'
        """)
        result = cur.fetchone()
        assert result is not None
        cur.close()
    
    def test_staging_schema_exists(self, db_conn):
        """Staging schema should exist."""
        cur = db_conn.cursor()
        cur.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name = 'staging'
        """)
        result = cur.fetchone()
        assert result is not None
        cur.close()
    
    def test_mart_schema_exists(self, db_conn):
        """Mart schema should exist."""
        cur = db_conn.cursor()
        cur.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name = 'mart'
        """)
        result = cur.fetchone()
        assert result is not None
        cur.close()
    
    def test_snapshots_schema_exists(self, db_conn):
        """Snapshots schema should exist."""
        cur = db_conn.cursor()
        cur.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name = 'snapshots'
        """)
        result = cur.fetchone()
        assert result is not None
        cur.close()


class TestBronzeTables:
    """Test bronze layer tables."""
    
    def test_bronze_tables_exist(self, db_conn):
        """All 8 bronze tables should exist."""
        cur = db_conn.cursor()
        
        expected_tables = [
            'orders', 'order_items', 'order_payments',
            'customers', 'sellers', 'products',
            'reviews', 'geolocation'
        ]
        
        for table in expected_tables:
            cur.execute(f"""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'bronze' 
                AND table_name = '{table}'
            """)
            result = cur.fetchone()
            assert result is not None, f"bronze.{table} should exist"
        
        cur.close()
    
    def test_bronze_tables_have_audit_columns(self, db_conn):
        """Bronze tables should have audit columns."""
        cur = db_conn.cursor()
        
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = 'bronze' 
            AND table_name = 'orders'
            AND column_name IN ('_ingested_at', '_source_file')
        """)
        audit_cols = [row[0] for row in cur.fetchall()]
        assert len(audit_cols) == 2
        
        cur.close()


class TestDDLFiles:
    """Test DDL file structure."""
    
    def test_ddl_directory_exists(self):
        """SQL DDL directory should exist."""
        assert SQL_DDL_DIR.exists()
        assert SQL_DDL_DIR.is_dir()
    
    def test_ddl_files_exist(self):
        """Required DDL files should exist."""
        required_files = [
            '01_create_bronze_schema.sql',
            '02_create_staging_schema.sql',
            '03_create_mart_dimensions.sql',
            '04_create_mart_facts.sql',
            '05_create_indexes.sql',
            '06_seed_reference_data.sql'
        ]
        
        for filename in required_files:
            filepath = SQL_DDL_DIR / filename
            assert filepath.exists(), f"{filename} should exist"
    
    def test_ddl_files_not_empty(self):
        """DDL files should contain SQL statements."""
        filepath = SQL_DDL_DIR / '01_create_bronze_schema.sql'
        content = filepath.read_text()
        assert len(content) > 0
        assert 'CREATE' in content.upper()
