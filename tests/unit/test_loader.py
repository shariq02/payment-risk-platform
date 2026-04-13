"""
Unit tests for load_bronze.py
==============================
Tests bronze layer loader functions.
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

sys.path.insert(0, str(PROJECT_ROOT / 'ingestion' / 'loader'))
from load_bronze import (
    deduplicate,
    add_audit_columns,
    convert_nan_to_none,
    FILE_TABLE_MAP,
    EXPECTED_ROW_COUNTS,
)


class TestDeduplicate:
    """Test deduplication logic."""
    
    def test_deduplicate_removes_duplicates(self):
        """deduplicate() should remove duplicate rows by primary key."""
        df = pd.DataFrame({
            'id': ['A', 'B', 'A', 'C'],
            'value': [1, 2, 3, 4]
        })
        result = deduplicate(df, ['id'], 'test_table')
        assert len(result) == 3
        assert 'A' in result['id'].values
        assert result[result['id'] == 'A']['value'].values[0] == 3
    
    def test_deduplicate_composite_key(self):
        """deduplicate() should handle composite primary keys."""
        df = pd.DataFrame({
            'order_id': ['1', '1', '2', '2'],
            'item_id': ['A', 'B', 'A', 'A'],
            'value': [10, 20, 30, 40]
        })
        result = deduplicate(df, ['order_id', 'item_id'], 'test_table')
        assert len(result) == 3
    
    def test_deduplicate_no_pk_returns_same(self):
        """deduplicate() with empty pk list should return df unchanged."""
        df = pd.DataFrame({'col': [1, 2, 3]})
        result = deduplicate(df, [], 'geolocation')
        assert len(result) == 3


class TestAuditColumns:
    """Test audit column addition."""
    
    def test_add_audit_columns_adds_both(self):
        """add_audit_columns() should add _ingested_at and _source_file."""
        df = pd.DataFrame({'col': [1, 2, 3]})
        result = add_audit_columns(df, 'test.csv')
        assert '_ingested_at' in result.columns
        assert '_source_file' in result.columns
    
    def test_add_audit_columns_source_file_value(self):
        """_source_file should contain the filename."""
        df = pd.DataFrame({'col': [1]})
        result = add_audit_columns(df, 'orders.csv')
        assert result['_source_file'].iloc[0] == 'orders.csv'
    
    def test_add_audit_columns_timestamp_populated(self):
        """_ingested_at should be populated with timestamp."""
        df = pd.DataFrame({'col': [1]})
        result = add_audit_columns(df, 'test.csv')
        assert pd.notna(result['_ingested_at'].iloc[0])


class TestNaNConversion:
    """Test NaN to NULL conversion."""
    
    def test_convert_nan_to_none_handles_float_nan(self):
        """convert_nan_to_none() should convert float NaN to None."""
        df = pd.DataFrame({'col': [1.0, np.nan, 3.0]})
        result = convert_nan_to_none(df)
        assert result['col'].iloc[1] is None
    
    def test_convert_nan_to_none_handles_string_nan(self):
        """convert_nan_to_none() should convert string 'NaN' to None."""
        df = pd.DataFrame({'col': ['value', 'NaN', 'other']})
        result = convert_nan_to_none(df)
        assert result['col'].iloc[1] is None
    
    def test_convert_nan_to_none_preserves_valid_values(self):
        """convert_nan_to_none() should not change valid values."""
        df = pd.DataFrame({'col': ['a', 'b', 'c']})
        result = convert_nan_to_none(df)
        assert result['col'].iloc[0] == 'a'
        assert result['col'].iloc[2] == 'c'


class TestConstants:
    """Test configuration constants."""
    
    def test_file_table_map_has_all_files(self):
        """FILE_TABLE_MAP should contain all 8 Olist CSV files."""
        assert len(FILE_TABLE_MAP) == 8
        assert 'olist_orders_dataset.csv' in FILE_TABLE_MAP
        assert 'olist_products_dataset.csv' in FILE_TABLE_MAP
    
    def test_file_table_map_structure(self):
        """FILE_TABLE_MAP entries should have table and primary_keys."""
        for filename, config in FILE_TABLE_MAP.items():
            assert 'table' in config
            assert 'primary_keys' in config
            assert isinstance(config['primary_keys'], list)
    
    def test_expected_row_counts_has_all_tables(self):
        """EXPECTED_ROW_COUNTS should have counts for all tables."""
        assert len(EXPECTED_ROW_COUNTS) == 8
        assert EXPECTED_ROW_COUNTS['orders'] == 99441
        assert EXPECTED_ROW_COUNTS['products'] == 32951
