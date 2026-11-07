"""
Unified Command Line Interface (CLI) Entrypoint
Supports database setup, dataset generation, pipeline execution,
backfills, reconciliation reports, test execution, and performance benchmarks.
"""

import argparse
import sys
from pathlib import Path
from decimal import Decimal
import pandas as pd
from tabulate import tabulate
from sqlalchemy import text

# Ensure project root is in sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from src.config import Config
from src.database import get_engine, init_database, seed_reference_data, test_connection
from src.data_generator import SyntheticDataGenerator, generate_starter_dataset
from src.pipeline import PipelineOrchestrator
from src.backfill import BackfillManager


def cmd_setup_db(args):
    """Initializes tables and seeds reference dimension records."""
    print("=== Initializing Database Schema & Reference Data ===")
    engine, engine_type = get_engine(use_fallback=True)
    init_database(engine, engine_type)
    seed_reference_data(engine, engine_type)
    status = test_connection()
    print(f"Backend Engine: {status['engine_type']}")
    print(f"Details: {status['details']}")
    print(f"Tables Initialized ({len(status['tables'])}): {', '.join(status['tables'])}")
    print("Database setup complete.")


def cmd_generate_data(args):
    """Generates synthetic e-commerce event streams."""
    output_path = Path(args.output) if args.output else (Config.GENERATED_DATA_DIR / f"transactions_{args.rows}.csv")
    print(f"Generating {args.rows} transaction events with seed={args.seed}...")
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
        source_file_name=output_path.name,
        prefix=args.prefix
    )
    saved = generator.save_to_csv(df, output_path)
    print(f"Dataset generated and saved to: {saved}")
    print(f"Summary: {meta}")


def cmd_run(args):
    """Executes the incremental data pipeline against an input CSV."""
    input_file = Path(args.input)
    if not input_file.exists():
        print(f"Error: Specified input file does not exist: {input_file}")
        sys.exit(1)

    print(f"Running pipeline for: {input_file} (Force: {args.force})")
    orchestrator = PipelineOrchestrator()
    result = orchestrator.run(input_file, force=args.force)

    print("\n=== PIPELINE EXECUTION SUMMARY ===")
    table_data = [[k, str(v)] for k, v in result.items() if k != "metrics_recomputed"]
    print(tabulate(table_data, headers=["Stage / Metric", "Value"], tablefmt="grid"))
    if "metrics_recomputed" in result:
        print(f"Metrics Recomputed: {result['metrics_recomputed']}")


def cmd_backfill(args):
    """Executes backfill recomputation for historical dates."""
    engine, engine_type = get_engine(use_fallback=True)
    manager = BackfillManager(engine, engine_type)

    print(f"Executing backfill from {args.start_date} to {args.end_date}...")
    res = manager.run_backfill(start_date=args.start_date, end_date=args.end_date)
    print("\n=== BACKFILL EXECUTION COMPLETE ===")
    print(f"Run ID: {res['run_id']}")
    print(f"Status: {res['status']}")
    print(f"Dates Recomputed ({len(res['target_dates'])}): {res['target_dates']}")
    print(f"Results: {res['results']}")


def cmd_report(args):
    """Displays latest analytical metrics and pipeline execution status."""
    engine, engine_type = get_engine(use_fallback=True)
    with engine.connect() as conn:
        print("\n--- LATEST DAILY SALES METRICS ---")
        df_sales = pd.read_sql("SELECT * FROM daily_sales_metrics ORDER BY business_date DESC LIMIT 10", conn)
        if not df_sales.empty:
            print(tabulate(df_sales, headers="keys", tablefmt="psql", showindex=False))
        else:
            print("No daily sales metrics recorded yet.")

        print("\n--- TOP PRODUCTS BY NET SALES ---")
        df_prod = pd.read_sql("""
            SELECT product_id, product_name, SUM(units_sold) AS units, SUM(net_sales) AS net_sales 
            FROM daily_product_metrics 
            GROUP BY product_id, product_name 
            ORDER BY net_sales DESC LIMIT 5
        """, conn)
        if not df_prod.empty:
            print(tabulate(df_prod, headers="keys", tablefmt="psql", showindex=False))

        print("\n--- RECENT PIPELINE RUN AUDIT LOGS ---")
        df_runs = pd.read_sql("SELECT run_id, pipeline_stage, status, records_read, records_valid, records_rejected, duration_seconds FROM pipeline_run_log ORDER BY start_time DESC LIMIT 5", conn)
        if not df_runs.empty:
            print(tabulate(df_runs, headers="keys", tablefmt="psql", showindex=False))


def cmd_reconcile(args):
    """Executes audit reconciliation queries across all stages."""
    engine, engine_type = get_engine(use_fallback=True)
    recon_script = Config.SQL_DIR / "reconciliation.sql"

    print("=== EXECUTING RECONCILIATION AUDIT ===")
    with engine.connect() as conn:
        raw_count = conn.execute(text("SELECT COUNT(*) FROM raw_transaction_events")).scalar() or 0
        proc_count = conn.execute(text("SELECT COUNT(*) FROM processed_transaction_events")).scalar() or 0
        rej_count = conn.execute(text("SELECT COUNT(*) FROM rejected_records")).scalar() or 0
        order_count = conn.execute(text("SELECT COUNT(*) FROM current_order_state")).scalar() or 0
        sales_total = conn.execute(text("SELECT COALESCE(SUM(net_sales), 0.00) FROM daily_sales_metrics")).scalar() or 0.0
        prod_total = conn.execute(text("SELECT COALESCE(SUM(net_sales), 0.00) FROM daily_product_metrics")).scalar() or 0.0
        cust_total = conn.execute(text("SELECT COALESCE(SUM(net_sales), 0.00) FROM daily_customer_metrics")).scalar() or 0.0

        audit_data = [
            ["Total Raw Ingested Events", raw_count],
            ["Total Processed Ledger Events", proc_count],
            ["Total Rejected Dead-Letter Records", rej_count],
            ["Accounted Events (Processed + Rejected)", proc_count + rej_count],
            ["Total Distinct Orders in Current State", order_count],
            ["Total Daily Sales Net Volume ($)", f"{float(sales_total):.2f}"],
            ["Total Product Metrics Net Volume ($)", f"{float(prod_total):.2f}"],
            ["Total Customer Metrics Net Volume ($)", f"{float(cust_total):.2f}"],
            ["Cross-Table Discrepancy (Sales - Products)", f"{abs(float(sales_total) - float(prod_total)):.4f}"],
            ["Cross-Table Discrepancy (Sales - Customers)", f"{abs(float(sales_total) - float(cust_total)):.4f}"]
        ]
        print(tabulate(audit_data, headers=["Reconciliation Check", "Measured Value"], tablefmt="grid"))


def cmd_test(args):
    """Runs the automated test suite via pytest."""
    import pytest
    print("=== Running Automated Test Suite via Pytest ===")
    test_dir = str(Config.PROJECT_ROOT / "tests")
    exit_code = pytest.main(["-v", test_dir])
    sys.exit(exit_code)


def cmd_performance(args):
    """Runs the performance benchmark script."""
    from scripts.performance_test import run_benchmark
    run_benchmark(rows=args.rows)


def cmd_dashboard(args):
    """Generates the interactive HTML executive dashboard."""
    from src.dashboard_generator import generate_html_dashboard
    output_path = Config.REPORTS_DIR / "dashboard.html"
    generate_html_dashboard(output_path)
    print(f"\n=== INTERACTIVE DASHBOARD GENERATED ===")
    print(f"Dashboard File: {output_path.resolve()}")
    print("Double-click this file or open it in any web browser to view the visual charts!")


def main():
    parser = argparse.ArgumentParser(
        description="Daily E-Commerce Sales & Customer Metrics Pipeline CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    subparsers = parser.add_subparsers(dest="command", required=True, help="Sub-commands")

    # setup-db
    subparsers.add_parser("setup-db", help="Initialize database schema and reference data")

    # generate-data
    p_gen = subparsers.add_parser("generate-data", help="Generate synthetic transaction datasets")
    p_gen.add_argument("--rows", type=int, default=1000, help="Row count")
    p_gen.add_argument("--customers", type=int, default=50, help="Customer count")
    p_gen.add_argument("--products", type=int, default=20, help="Product count")
    p_gen.add_argument("--start-date", type=str, default="2026-01-01", help="Start date (YYYY-MM-DD)")
    p_gen.add_argument("--days", type=int, default=14, help="Days span")
    p_gen.add_argument("--invalid-pct", type=float, default=0.05, help="Invalid records ratio")
    p_gen.add_argument("--duplicate-pct", type=float, default=0.03, help="Duplicates ratio")
    p_gen.add_argument("--late-pct", type=float, default=0.05, help="Late events ratio")
    p_gen.add_argument("--seed", type=int, default=42, help="Random seed")
    p_gen.add_argument("--output", type=str, default=None, help="Output file path")
    p_gen.add_argument("--prefix", type=str, default=None, help="Batch prefix for event and order identifiers")

    # run
    p_run = subparsers.add_parser("run", help="Run the pipeline on input CSV")
    p_run.add_argument("--input", type=str, required=True, help="Input CSV path")
    p_run.add_argument("--force", action="store_true", help="Force reprocessing even if file was previously ingested")

    # backfill
    p_bf = subparsers.add_parser("backfill", help="Recalculate metrics for historical dates")
    p_bf.add_argument("--start-date", type=str, required=True, help="Start date (YYYY-MM-DD)")
    p_bf.add_argument("--end-date", type=str, required=True, help="End date (YYYY-MM-DD)")

    # report
    subparsers.add_parser("report", help="Display summary reports from database")

    # reconcile
    subparsers.add_parser("reconcile", help="Run audit reconciliation checks")

    # test
    subparsers.add_parser("test", help="Execute pytest automated test suite")

    # performance
    p_perf = subparsers.add_parser("performance", help="Run performance benchmark")
    p_perf.add_argument("--rows", type=int, default=10000, help="Records to benchmark")

    # dashboard
    subparsers.add_parser("dashboard", help="Generate interactive HTML executive dashboard")

    args = parser.parse_args()

    commands = {
        "setup-db": cmd_setup_db,
        "generate-data": cmd_generate_data,
        "run": cmd_run,
        "backfill": cmd_backfill,
        "report": cmd_report,
        "reconcile": cmd_reconcile,
        "test": cmd_test,
        "performance": cmd_performance,
        "dashboard": cmd_dashboard
    }

    cmd_fn = commands.get(args.command)
    if cmd_fn:
        cmd_fn(args)


if __name__ == "__main__":
    main()
