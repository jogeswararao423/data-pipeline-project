"""
Pytest Test Configuration and Shared Fixtures
Provides isolated in-memory/temporary SQLite database engines for high-speed testing.
"""

import os
import sys
from pathlib import Path
import pytest
from sqlalchemy import create_engine, text

# Add project root to sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from src.database import init_database, seed_reference_data
from src.config import Config


@pytest.fixture(scope="function")
def test_engine(tmp_path):
    """Creates a clean, isolated SQLite test database for each test function."""
    db_file = tmp_path / "test_pipeline.db"
    engine = create_engine(f"sqlite:///{db_file.as_posix()}", echo=False)
    
    with engine.connect() as conn:
        conn.execute(text("PRAGMA foreign_keys = ON;"))
        conn.commit()

    init_database(engine, "sqlite")
    seed_reference_data(engine, "sqlite")
    
    yield engine
    
    engine.dispose()


@pytest.fixture
def sample_valid_row():
    """Returns a valid event row dictionary."""
    return {
        "event_id": "EVT-TEST-0001",
        "order_id": "ORD-TEST-0001",
        "customer_id": "CUST-1001",
        "product_id": "PROD-2001",
        "event_type": "CREATE",
        "quantity": 2,
        "unit_price": 149.99,
        "total_amount": 299.98,
        "event_timestamp": "2026-01-05 10:00:00",
        "business_date": "2026-01-05",
        "source_file": "test.csv",
        "source_version": 1,
        "run_id": "TEST-RUN"
    }
