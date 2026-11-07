#!/usr/bin/env python3
"""
CLI Data Generation Utility
Enables flexible generation of reproducible synthetic datasets from 100 to 1,000,000+ rows.
"""

import argparse
import sys
from pathlib import Path

# Add project root to sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from src.data_generator import SyntheticDataGenerator
from src.config import Config


def main():
    parser = argparse.ArgumentParser(description="Synthetic E-Commerce Event Data Generator")
    parser.add_argument("--rows", type=int, default=1000, help="Total transaction events to generate")
    parser.add_argument("--customers", type=int, default=50, help="Number of distinct customers")
    parser.add_argument("--products", type=int, default=20, help="Number of distinct products")
    parser.add_argument("--start-date", type=str, default="2026-01-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--days", type=int, default=14, help="Span of days across dataset")
    parser.add_argument("--invalid-pct", type=float, default=0.05, help="Percentage of invalid records (0.0 to 1.0)")
    parser.add_argument("--duplicate-pct", type=float, default=0.03, help="Percentage of duplicate events (0.0 to 1.0)")
    parser.add_argument("--late-pct", type=float, default=0.05, help="Percentage of late-arriving events")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    parser.add_argument("--output", type=str, default=None, help="Output CSV file path")

    args = parser.parse_args()

    output_path = Path(args.output) if args.output else (Config.GENERATED_DATA_DIR / f"transactions_{args.rows}.csv")
    print(f"Generating {args.rows} events with seed={args.seed}...")

    generator = SyntheticDataGenerator(
        seed=args.seed,
        customer_count=args.customers,
        product_count=args.products,
        start_date=args.start_date,
        days_span=args.days
    )

    df, meta = generator.generate_events(
        total_rows=args.rows,
        invalid_pct=args.invalid_pct,
        duplicate_pct=args.duplicate_pct,
        late_pct=args.late_pct,
        source_file_name=output_path.name
    )

    saved_path = generator.save_to_csv(df, output_path)
    print(f"Dataset successfully generated and saved to: {saved_path}")
    print(f"Summary: Total={meta['total_generated']}, Base Orders={meta['valid_base_orders']}, Lifecycle Events={meta['lifecycle_events']}, Duplicates={meta['duplicates_injected']}, Invalids={meta['invalid_injected']}")


if __name__ == "__main__":
    main()
