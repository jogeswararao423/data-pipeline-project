"""
Configuration and Environment Settings Module
Provides centralized, validated configuration with support for MySQL production
and SQLite offline / testing modes.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Base Project Directory (always points to ecommerce-data-pipeline root)
BASE_DIR = Path(__file__).resolve().parent.parent

# Load .env file if present
ENV_PATH = BASE_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH)


class Config:
    """Centralized Pipeline Configuration Settings."""

    # Project Directories
    PROJECT_ROOT: Path = BASE_DIR
    CONFIG_DIR: Path = BASE_DIR / "config"
    DATA_DIR: Path = BASE_DIR / "data"
    RAW_DATA_DIR: Path = BASE_DIR / os.getenv("RAW_DATA_DIR", "data/raw")
    SAMPLE_DATA_DIR: Path = BASE_DIR / os.getenv("SAMPLE_DATA_DIR", "data/sample")
    REJECTED_DATA_DIR: Path = BASE_DIR / os.getenv("REJECTED_DATA_DIR", "data/rejected")
    GENERATED_DATA_DIR: Path = BASE_DIR / os.getenv("GENERATED_DATA_DIR", "data/generated")
    
    OUTPUTS_DIR: Path = BASE_DIR / "outputs"
    REPORTS_DIR: Path = BASE_DIR / os.getenv("REPORTS_DIR", "outputs/reports")
    VALIDATION_DIR: Path = BASE_DIR / os.getenv("VALIDATION_DIR", "outputs/validation")
    SAMPLE_RESULTS_DIR: Path = BASE_DIR / os.getenv("SAMPLE_RESULTS_DIR", "outputs/sample_results")
    LOGS_DIR: Path = BASE_DIR / "outputs/logs"
    SQL_DIR: Path = BASE_DIR / "sql"

    # Database Configuration
    DB_ENGINE: str = os.getenv("DB_ENGINE", "sqlite").lower()
    
    # MySQL Settings
    MYSQL_HOST: str = os.getenv("MYSQL_HOST", "localhost")
    MYSQL_PORT: int = int(os.getenv("MYSQL_PORT", 3306))
    MYSQL_USER: str = os.getenv("MYSQL_USER", "root")
    MYSQL_PASSWORD: str = os.getenv("MYSQL_PASSWORD", "")
    MYSQL_DATABASE: str = os.getenv("MYSQL_DATABASE", "ecommerce_pipeline")
    MYSQL_CHARSET: str = os.getenv("MYSQL_CHARSET", "utf8mb4")

    # SQLite Settings (zero-setup offline engine and integration testing)
    SQLITE_DB_PATH: Path = BASE_DIR / os.getenv("SQLITE_DB_PATH", "data/ecommerce_pipeline.db")

    # Operational Parameters
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", 5000))
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", 3))
    RETRY_BACKOFF_SECONDS: float = float(os.getenv("RETRY_BACKOFF_SECONDS", 2.0))

    # Synthetic Data Generator Defaults
    RANDOM_SEED: int = int(os.getenv("RANDOM_SEED", 42))
    DEFAULT_ROW_COUNT: int = int(os.getenv("DEFAULT_ROW_COUNT", 1000))
    INVALID_RECORD_PCT: float = float(os.getenv("INVALID_RECORD_PCT", 0.05))
    DUPLICATE_RECORD_PCT: float = float(os.getenv("DUPLICATE_RECORD_PCT", 0.03))
    LATE_EVENT_PCT: float = float(os.getenv("LATE_EVENT_PCT", 0.05))
    CUSTOMER_COUNT: int = int(os.getenv("CUSTOMER_COUNT", 50))
    PRODUCT_COUNT: int = int(os.getenv("PRODUCT_COUNT", 20))

    @classmethod
    def ensure_directories(cls) -> None:
        """Create all required data and output directories if they do not exist."""
        for path in [
            cls.RAW_DATA_DIR,
            cls.SAMPLE_DATA_DIR,
            cls.REJECTED_DATA_DIR,
            cls.GENERATED_DATA_DIR,
            cls.REPORTS_DIR,
            cls.VALIDATION_DIR,
            cls.SAMPLE_RESULTS_DIR,
            cls.LOGS_DIR
        ]:
            path.mkdir(parents=True, exist_ok=True)


# Initialize runtime directories upon import
Config.ensure_directories()
