"""
Performance test for dbt model execution
=========================================
Ensures models run under time thresholds.
"""

import sys
from pathlib import Path
import subprocess
import time
import os

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import DBT_DIR

# Expand home directory
PROFILES_DIR = os.path.expanduser('~/.dbt')


class TestDbtPerformance:
    """Test dbt model performance."""
    
    def test_full_pipeline_under_threshold(self):
        """Full dbt run should complete under 60 seconds."""
        start = time.time()
        
        result = subprocess.run(
            ['dbt', 'run', '--profiles-dir', PROFILES_DIR, '--project-dir', str(DBT_DIR)],
            capture_output=True,
            text=True
        )
        
        elapsed = time.time() - start
        
        assert result.returncode == 0, "dbt run should succeed"
        assert elapsed < 60, f"dbt run took {elapsed:.2f}s, should be under 60s"
    
    def test_staging_models_under_threshold(self):
        """Staging models should complete under 10 seconds."""
        start = time.time()
        
        result = subprocess.run(
            ['dbt', 'run', '--select', 'staging', '--profiles-dir', PROFILES_DIR, '--project-dir', str(DBT_DIR)],
            capture_output=True,
            text=True
        )
        
        elapsed = time.time() - start
        
        assert result.returncode == 0
        assert elapsed < 10, f"Staging took {elapsed:.2f}s, should be under 10s"
    
    def test_fact_models_under_threshold(self):
        """Fact models should complete under 30 seconds."""
        start = time.time()
        
        result = subprocess.run(
            ['dbt', 'run', '--select', 'marts.facts', '--profiles-dir', PROFILES_DIR, '--project-dir', str(DBT_DIR)],
            capture_output=True,
            text=True
        )
        
        elapsed = time.time() - start
        
        assert result.returncode == 0
        assert elapsed < 30, f"Facts took {elapsed:.2f}s, should be under 30s"
    
    def test_dimension_models_under_threshold(self):
        """Dimension models should complete under 20 seconds."""
        start = time.time()
        
        result = subprocess.run(
            ['dbt', 'run', '--select', 'marts.dimensions', '--profiles-dir', PROFILES_DIR, '--project-dir', str(DBT_DIR)],
            capture_output=True,
            text=True
        )
        
        elapsed = time.time() - start
        
        assert result.returncode == 0
        assert elapsed < 20, f"Dimensions took {elapsed:.2f}s, should be under 20s"
    
    def test_snapshot_under_threshold(self):
        """Snapshots should complete under 15 seconds."""
        start = time.time()
        
        result = subprocess.run(
            ['dbt', 'snapshot', '--profiles-dir', PROFILES_DIR, '--project-dir', str(DBT_DIR)],
            capture_output=True,
            text=True
        )
        
        elapsed = time.time() - start
        
        assert result.returncode == 0
        assert elapsed < 15, f"Snapshots took {elapsed:.2f}s, should be under 15s"


class TestDbtTestPerformance:
    """Test dbt test performance."""
    
    def test_all_tests_under_threshold(self):
        """All dbt tests should complete under 45 seconds."""
        start = time.time()
        
        result = subprocess.run(
            ['dbt', 'test', '--profiles-dir', PROFILES_DIR, '--project-dir', str(DBT_DIR)],
            capture_output=True,
            text=True
        )
        
        elapsed = time.time() - start
        
        assert result.returncode == 0
        assert elapsed < 45, f"dbt test took {elapsed:.2f}s, should be under 45s"


class TestDbtCompilationPerformance:
    """Test dbt compilation performance."""
    
    def test_compile_under_threshold(self):
        """dbt compile should complete under 5 seconds."""
        start = time.time()
        
        result = subprocess.run(
            ['dbt', 'compile', '--profiles-dir', PROFILES_DIR, '--project-dir', str(DBT_DIR)],
            capture_output=True,
            text=True
        )
        
        elapsed = time.time() - start
        
        assert result.returncode == 0
        assert elapsed < 5, f"dbt compile took {elapsed:.2f}s, should be under 5s"
