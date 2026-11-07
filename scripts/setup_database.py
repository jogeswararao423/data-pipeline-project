#!/usr/bin/env python3
"""
Standalone Database Setup and Verification Script
"""

import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from src.database import get_engine, init_database, seed_reference_data, test_connection


def main():
    print("=== Setting up Database Schema and Seeding Reference Dimensions ===")
    engine, engine_type = get_engine(use_fallback=True)
    init_database(engine, engine_type)
    seed_reference_data(engine, engine_type)
    status = test_connection()
    print(f"Backend Engine: {status['engine_type']}")
    print(f"Connection Status: {'CONNECTED' if status['connected'] else 'FAILED'}")
    print(f"Details: {status['details']}")
    print(f"Total Tables Created: {len(status['tables'])}")
    for tbl in status['tables']:
        print(f"  - {tbl}")
    print("Database setup successfully completed.")


if __name__ == "__main__":
    main()
