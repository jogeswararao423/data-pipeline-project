#!/usr/bin/env python3
"""
Standalone Pipeline Execution Runner Script
"""

import argparse
import sys
from pathlib import Path
from tabulate import tabulate

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from src.config import Config
from src.pipeline import PipelineOrchestrator


def main():
    parser = argparse.ArgumentParser(description="Execute E-Commerce Pipeline")
    parser.add_argument("--input", type=str, default=None, help="Input CSV path (defaults to sample dataset)")
    parser.add_argument("--force", action="store_true", help="Force ingestion even if previously processed")
    args = parser.parse_args()

    input_file = Path(args.input) if args.input else (Config.SAMPLE_DATA_DIR / "sample_transactions.csv")
    if not input_file.exists():
        print(f"Error: Input file does not exist: {input_file}")
        sys.exit(1)

    print(f"Starting pipeline execution for: {input_file}")
    orchestrator = PipelineOrchestrator()
    summary = orchestrator.run(input_file, force=args.force)

    print("\n" + "=" * 60)
    print("  PIPELINE EXECUTION COMPLETE")
    print("=" * 60)
    display_rows = [[k, str(v)] for k, v in summary.items() if k != "metrics_recomputed"]
    print(tabulate(display_rows, headers=["Stage / Metric", "Value"], tablefmt="grid"))
    if "metrics_recomputed" in summary:
        print(f"\nMetrics Recomputed: {summary['metrics_recomputed']}")


if __name__ == "__main__":
    main()
