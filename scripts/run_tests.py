#!/usr/bin/env python3
"""
Automated Test Runner Wrapper
Runs the complete pytest suite and outputs detailed execution logs.
"""

import sys
from pathlib import Path
import pytest

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from src.config import Config


def main():
    print("=" * 65)
    print("  LAUNCHING TEST SUITE VIA PYTEST")
    print("=" * 65)
    test_path = str(Config.PROJECT_ROOT / "tests")
    args = ["-v", "-s", "--tb=short", test_path]
    exit_code = pytest.main(args)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
