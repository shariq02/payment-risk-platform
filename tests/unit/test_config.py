"""
Unit tests for config.py
========================
Tests configuration loading, validation, and path resolution.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import (
    DATABASE_CONFIG,
    validate_config,
    get_database_url,
    get_dbt_run_command,
    get_dbt_test_command,
    get_dbt_snapshot_command,
    PROJECT_ROOT as CONFIG_PROJECT_ROOT,
    DBT_DIR,
    API_DIR,
)


class TestDatabaseConfig:
    """Test database configuration."""
    
    def test_database_config_exists(self):
        """DATABASE_CONFIG should be a dict with required keys."""
        assert isinstance(DATABASE_CONFIG, dict)
        required_keys = ['host', 'port', 'database', 'user', 'password']
        for key in required_keys:
            assert key in DATABASE_CONFIG
    
    def test_database_config_types(self):
        """DATABASE_CONFIG values should have correct types."""
        assert isinstance(DATABASE_CONFIG['host'], str)
        assert isinstance(DATABASE_CONFIG['port'], int)
        assert isinstance(DATABASE_CONFIG['database'], str)
        assert isinstance(DATABASE_CONFIG['user'], str)
    
    def test_get_database_url(self):
        """get_database_url() should return valid connection string."""
        if DATABASE_CONFIG['password']:
            url = get_database_url()
            assert url.startswith('postgresql://')
            assert DATABASE_CONFIG['user'] in url
            assert DATABASE_CONFIG['database'] in url
            assert str(DATABASE_CONFIG['port']) in url


class TestConfigValidation:
    """Test configuration validation."""
    
    def test_validate_config_with_password(self):
        """validate_config() should pass if password is set."""
        if DATABASE_CONFIG['password']:
            assert validate_config() is True
    
    def test_validate_config_catches_missing_password(self, monkeypatch):
        """validate_config() should fail if password missing."""
        monkeypatch.setitem(DATABASE_CONFIG, 'password', None)
        assert validate_config() is False


class TestPathResolution:
    """Test path configuration."""
    
    def test_project_root_exists(self):
        """PROJECT_ROOT should point to actual project directory."""
        assert CONFIG_PROJECT_ROOT.exists()
        assert CONFIG_PROJECT_ROOT.is_dir()
    
    def test_dbt_dir_exists(self):
        """DBT_DIR should exist."""
        assert DBT_DIR.exists()
        assert DBT_DIR.is_dir()
        assert (DBT_DIR / 'dbt_project.yml').exists()
    
    def test_api_dir_exists(self):
        """API_DIR should exist."""
        assert API_DIR.exists()
        assert API_DIR.is_dir()


class TestDbtCommands:
    """Test dbt command builders."""
    
    def test_dbt_run_command_no_select(self):
        """get_dbt_run_command() without select."""
        cmd = get_dbt_run_command()
        assert 'dbt run' in cmd
        assert '--project-dir' in cmd
        assert '--profiles-dir' in cmd
    
    def test_dbt_run_command_with_select(self):
        """get_dbt_run_command() with select."""
        cmd = get_dbt_run_command(select='staging')
        assert 'dbt run' in cmd
        assert '--select staging' in cmd
    
    def test_dbt_test_command_no_select(self):
        """get_dbt_test_command() without select."""
        cmd = get_dbt_test_command()
        assert 'dbt test' in cmd
        assert '--project-dir' in cmd
    
    def test_dbt_test_command_with_select(self):
        """get_dbt_test_command() with select."""
        cmd = get_dbt_test_command(select='marts')
        assert 'dbt test' in cmd
        assert '--select marts' in cmd
    
    def test_dbt_snapshot_command(self):
        """get_dbt_snapshot_command() should return snapshot command."""
        cmd = get_dbt_snapshot_command()
        assert 'dbt snapshot' in cmd
        assert '--project-dir' in cmd
        assert '--profiles-dir' in cmd
