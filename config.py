"""
Configuration settings for Payment Risk and Order Analytics Platform.
ALL SENSITIVE DATA IN .env FILE - NEVER COMMIT .env TO GITHUB
"""

import os
import logging
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables first
load_dotenv()

# ====================================================================
# PROJECT ROOT
# ====================================================================
PROJECT_ROOT = Path(__file__).parent

# ====================================================================
# DIRECTORY STRUCTURE
# ====================================================================

# Data directories
DATA_DIR          = PROJECT_ROOT / "data"
RAW_DATA_DIR      = DATA_DIR / "raw"
EXPLORATION_DIR   = DATA_DIR / "exploration"

# Ingestion
INGESTION_DIR     = PROJECT_ROOT / "ingestion"
LOADER_DIR        = INGESTION_DIR / "loader"

# dbt
DBT_DIR           = PROJECT_ROOT / "dbt"
DBT_MODELS_DIR    = DBT_DIR / "models"
DBT_SNAPSHOTS_DIR = DBT_DIR / "snapshots"
DBT_TESTS_DIR     = DBT_DIR / "tests"
DBT_MACROS_DIR    = DBT_DIR / "macros"
DBT_TARGET_DIR    = DBT_DIR / "target"

# Airflow
AIRFLOW_DIR       = PROJECT_ROOT / "airflow"
AIRFLOW_DAGS_DIR  = AIRFLOW_DIR / "dags"

# SQL
SQL_DIR           = PROJECT_ROOT / "sql"
SQL_DDL_DIR       = SQL_DIR / "ddl"
SQL_ANALYTICS_DIR = SQL_DIR / "analytics"

# Apps
APPS_DIR          = PROJECT_ROOT / "apps"
API_DIR           = APPS_DIR / "risk_api"

# Docs
DOCS_DIR          = PROJECT_ROOT / "docs"

# Logs
LOGS_DIR          = PROJECT_ROOT / "logs"

# Tests
TESTS_DIR         = PROJECT_ROOT / "tests"
UNIT_TESTS_DIR    = TESTS_DIR / "unit"
INTEGRATION_TESTS_DIR = TESTS_DIR / "integration"

# Auto-create all directories on import
for directory in [
    RAW_DATA_DIR,
    EXPLORATION_DIR,
    LOADER_DIR,
    DBT_MODELS_DIR,
    DBT_SNAPSHOTS_DIR,
    DBT_TESTS_DIR,
    DBT_MACROS_DIR,
    AIRFLOW_DAGS_DIR,
    SQL_DDL_DIR,
    SQL_ANALYTICS_DIR,
    API_DIR,
    DOCS_DIR,
    LOGS_DIR,
    UNIT_TESTS_DIR,
    INTEGRATION_TESTS_DIR,
]:
    directory.mkdir(parents=True, exist_ok=True)

# ====================================================================
# DATABASE CONFIGURATION - ALL FROM .env
# ====================================================================
DATABASE_CONFIG = {
    'host':     os.getenv('POSTGRES_HOST', 'localhost'),
    'port':     int(os.getenv('POSTGRES_PORT', 5432)),
    'database': os.getenv('POSTGRES_DB', 'payment_risk_platform'),
    'user':     os.getenv('POSTGRES_USER', 'postgres'),
    'password': os.getenv('POSTGRES_PASSWORD'),  # NO DEFAULT - MUST BE IN .env
}


def get_database_url() -> str:
    """Get PostgreSQL connection URL from environment variables."""
    if not DATABASE_CONFIG['password']:
        raise ValueError("POSTGRES_PASSWORD must be set in .env file")
    return (
        f"postgresql://{DATABASE_CONFIG['user']}:"
        f"{DATABASE_CONFIG['password']}@"
        f"{DATABASE_CONFIG['host']}:"
        f"{DATABASE_CONFIG['port']}/"
        f"{DATABASE_CONFIG['database']}"
    )


# ====================================================================
# AIRFLOW CONFIGURATION
# ====================================================================
AIRFLOW_CONFIG = {
    'home':         os.getenv('AIRFLOW_HOME', str(Path.home() / 'airflow')),
    'load_examples': os.getenv('AIRFLOW__CORE__LOAD_EXAMPLES', 'False'),
    'dags_dir':     str(AIRFLOW_DAGS_DIR),
}

# ====================================================================
# DBT CONFIGURATION
# ====================================================================
DBT_CONFIG = {
    'project_dir':  str(DBT_DIR),
    'profiles_dir': os.getenv('DBT_PROFILES_DIR', str(Path.home() / '.dbt')),
    'target':       os.getenv('DBT_TARGET', 'dev'),
    'project_name': 'payment_risk_platform',
}


def get_dbt_run_command(select: str = None) -> str:
    """Build dbt run command string for use in Airflow BashOperator."""
    cmd = (
        f"dbt run"
        f" --project-dir {DBT_CONFIG['project_dir']}"
        f" --profiles-dir {DBT_CONFIG['profiles_dir']}"
    )
    if select:
        cmd += f" --select {select}"
    return cmd


def get_dbt_test_command(select: str = None) -> str:
    """Build dbt test command string for use in Airflow BashOperator."""
    cmd = (
        f"dbt test"
        f" --project-dir {DBT_CONFIG['project_dir']}"
        f" --profiles-dir {DBT_CONFIG['profiles_dir']}"
    )
    if select:
        cmd += f" --select {select}"
    return cmd


def get_dbt_snapshot_command() -> str:
    """Build dbt snapshot command string for use in Airflow BashOperator."""
    return (
        f"dbt snapshot"
        f" --project-dir {DBT_CONFIG['project_dir']}"
        f" --profiles-dir {DBT_CONFIG['profiles_dir']}"
    )


# ====================================================================
# API CONFIGURATION
# ====================================================================
API_CONFIG = {
    'host':    os.getenv('API_HOST', '0.0.0.0'),
    'port':    int(os.getenv('API_PORT', 8000)),
}

# ====================================================================
# RISK THRESHOLD CONFIGURATION - ALL FROM .env WITH SENSIBLE DEFAULTS
# ====================================================================
RISK_CONFIG = {
    'high_value_threshold':          float(os.getenv('HIGH_VALUE_THRESHOLD', 500)),
    'high_risk_score_threshold':     float(os.getenv('HIGH_RISK_SCORE_THRESHOLD', 0.7)),
    'seller_late_delivery_threshold': float(os.getenv('SELLER_LATE_DELIVERY_THRESHOLD', 0.15)),
    'seller_min_orders_threshold':   int(os.getenv('SELLER_MIN_ORDERS_THRESHOLD', 5)),
    'dispute_proxy_review_score':    int(os.getenv('DISPUTE_PROXY_REVIEW_SCORE', 2)),
}

# ====================================================================
# BRONZE SCHEMA CONFIGURATION
# ====================================================================
BRONZE_SCHEMA = 'bronze'
STAGING_SCHEMA = 'staging'
MART_SCHEMA = 'mart'
SNAPSHOTS_SCHEMA = 'snapshots'

BRONZE_TABLES = [
    'orders',
    'order_items',
    'order_payments',
    'customers',
    'sellers',
    'products',
    'reviews',
    'geolocation',
    'product_category_translation',
]

# ====================================================================
# LOGGING CONFIGURATION
# ====================================================================
LOGGING_CONFIG = {
    'level':  os.getenv('LOG_LEVEL', 'INFO'),
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'app_log':      LOGS_DIR / 'app.log',
    'error_log':    LOGS_DIR / 'error.log',
    'pipeline_log': LOGS_DIR / 'pipeline.log',
}


def get_logger(name: str) -> logging.Logger:
    """
    Return a named logger writing to console and logs/app.log.

    Usage:
        from config import get_logger
        logger = get_logger(__name__)
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger  # already configured, avoid duplicate handlers

    level = getattr(logging, LOGGING_CONFIG['level'].upper(), logging.INFO)
    fmt = logging.Formatter(LOGGING_CONFIG['format'])

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(level)
    ch.setFormatter(fmt)

    # App log file handler
    fh = logging.FileHandler(LOGGING_CONFIG['app_log'], encoding='utf-8')
    fh.setLevel(level)
    fh.setFormatter(fmt)

    # Error log file handler - errors and above only
    eh = logging.FileHandler(LOGGING_CONFIG['error_log'], encoding='utf-8')
    eh.setLevel(logging.ERROR)
    eh.setFormatter(fmt)

    logger.setLevel(level)
    logger.addHandler(ch)
    logger.addHandler(fh)
    logger.addHandler(eh)
    logger.propagate = False

    return logger


# ====================================================================
# VALIDATION
# ====================================================================
def validate_config() -> bool:
    """Validate critical configuration settings."""
    errors = []

    if not DATABASE_CONFIG['password']:
        errors.append("POSTGRES_PASSWORD not set in .env")

    if not DBT_CONFIG['profiles_dir']:
        errors.append("DBT_PROFILES_DIR not set")

    if errors:
        print("Configuration errors:")
        for error in errors:
            print(f"  - {error}")
        return False

    return True


# ====================================================================
# MAIN - RUN DIRECTLY TO VERIFY CONFIG
# ====================================================================
if __name__ == '__main__':
    print("Configuration loaded successfully")
    print(f"Project root:    {PROJECT_ROOT}")
    print(f"Database:        {DATABASE_CONFIG['database']}")
    print(f"Database host:   {DATABASE_CONFIG['host']}")
    print(f"Airflow home:    {AIRFLOW_CONFIG['home']}")
    print(f"dbt project dir: {DBT_CONFIG['project_dir']}")
    print(f"dbt profiles:    {DBT_CONFIG['profiles_dir']}")
    print(f"Logs dir:        {LOGS_DIR}")

    if validate_config():
        print("\nConfiguration validation: PASSED")
    else:
        print("\nConfiguration validation: FAILED")
