"""
Structured Operational Logging Configuration
Provides unified console and file logging with ISO timestamps and log levels.
"""

import logging
import sys
from pathlib import Path
from src.config import Config


def setup_logger(name: str = "ecommerce_pipeline") -> logging.Logger:
    """Configures structured console and file handlers for pipeline execution."""
    logger = logging.getLogger(name)
    level = getattr(logging, Config.LOG_LEVEL, logging.INFO)
    logger.setLevel(level)

    # Avoid duplicate handlers if already configured
    if logger.handlers:
        return logger

    # Formatters
    log_format = "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(log_format, datefmt=date_format)

    # Console Handler (stdout)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File Handler
    log_file = Config.LOGS_DIR / "pipeline.log"
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger
