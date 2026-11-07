#!/usr/bin/env python3
"""
Performance Measurement and Scalability Benchmark Script
Measures throughput, stage-level latencies, memory footprint,
and demonstrates key optimizations across varying dataset sizes.
"""

import argparse
import os
import platform
import sys
import time
from pathlib import Path
from decimal import Decimal
import pandas as pd
import psutil
from tabulate import tabulate

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from src.config import Config
from src.database import get_engine, init_database, seed_reference_data
from src.data_generator import SyntheticDataGenerator
from src.ingestion import FileIngestionManager
from src.validation import DataValidator
from src.transformation import normalize_event_dataframe
from src.event_processor import EventProcessor
from src.metrics import MetricsAggregator


def get_memory_usage_mb() -> float:
    """Returns current process RSS memory consumption in MB."""
    process = psutil.Process(os.getpid())
    return round(process.memory_info().rss / (1024 * 1024), 2)


def run_benchmark(rows: int = 10000, seed: int = 42) -> dict:
    """Runs a full timed performance benchmark across pipeline stages."""
    print("\n" + "=" * 75)
    print(f"  PERFORMANCE BENCHMARK: {rows:,} EVENT RECORDS")
    print("=" * 75)

    env_info = {
        "OS": f"{platform.system()} {platform.release()}",
        "Python": platform.python_version(),
        "CPU Cores": os.cpu_count(),
        "Total RAM": f"{round(psutil.virtual_memory().total / (1024**3), 2)} GB"
    }
    print("Environment Specifications:")
    for k, v in env_info.items():
        print(f"  - {k}: {v}")

    mem_start = get_memory_usage_mb()
    engine, engine_type = get_engine(use_fallback=True)
    init_database(engine, engine_type)
    seed_reference_data(engine, engine_type)

    # 1. Dataset Generation
    t0 = time.time()
    benchmark_seed = seed or int(time.time())
    temp_csv = Config.GENERATED_DATA_DIR / f"perf_test_{rows}_{int(time.time())}.csv"
    generator = SyntheticDataGenerator(seed=benchmark_seed, days_span=10)
    df_gen, gen_meta = generator.generate_events(
        total_rows=rows,
        invalid_pct=0.03,
        duplicate_pct=0.02,
        late_pct=0.04,
        source_file_name=temp_csv.name
    )
    # Ensure unique event IDs for benchmark runs
    run_tag = int(time.time() * 1000) % 1000000
    df_gen["event_id"] = df_gen["event_id"].apply(lambda eid: f"{eid}-B{run_tag}")
    generator.save_to_csv(df_gen, temp_csv)
    gen_time = round(time.time() - t0, 3)

    # 2. Ingestion
    t1 = time.time()
    ingestion_mgr = FileIngestionManager(engine, engine_type)
    df_raw, ingest_meta = ingestion_mgr.ingest_file(temp_csv, run_id=f"PERF-{rows}", force=True)
    ingest_time = round(time.time() - t1, 3)

    # 3. Validation
    t2 = time.time()
    validator = DataValidator(engine)
    df_valid, df_rej, val_summary = validator.validate_records(
        df_raw,
        run_id=f"PERF-{rows}",
        source_file_name=temp_csv.name
    )
    val_time = round(time.time() - t2, 3)

    # 4. Transformation
    t3 = time.time()
    df_norm = normalize_event_dataframe(df_valid)
    transform_time = round(time.time() - t3, 3)

    # 5. Database Ledger Load & State Folding
    t4 = time.time()
    processor = EventProcessor(engine, engine_type)
    events_persisted = processor.persist_processed_events(df_norm)
    orders_ins, orders_upd, affected_dates = processor.update_order_states_for_batch(df_norm)
    db_load_time = round(time.time() - t4, 3)

    # 6. Aggregation
    t5 = time.time()
    aggregator = MetricsAggregator(engine, engine_type)
    metrics_res = aggregator.recompute_daily_metrics_for_dates(affected_dates, f"PERF-{rows}")
    agg_time = round(time.time() - t5, 3)

    # Total pipeline processing time (excluding synthetic generation)
    total_pipeline_time = round(ingest_time + val_time + transform_time + db_load_time + agg_time, 3)
    throughput_rps = round(rows / total_pipeline_time, 2) if total_pipeline_time > 0 else 0
    mem_end = get_memory_usage_mb()
    mem_delta = round(mem_end - mem_start, 2)

    perf_table = [
        ["Dataset Generation", f"{gen_time:.3f} s", f"{round(rows / max(gen_time, 0.0001), 1):,} rows/s"],
        ["CSV Ingestion & Staging", f"{ingest_time:.3f} s", f"{round(rows / max(ingest_time, 0.0001), 1):,} rows/s"],
        ["Data Validation & DQ Routing", f"{val_time:.3f} s", f"{round(rows / max(val_time, 0.0001), 1):,} rows/s"],
        ["Transformation & Normalization", f"{transform_time:.3f} s", f"{round(rows / max(transform_time, 0.0001), 1):,} rows/s"],
        ["Ledger Persist & Order State Fold", f"{db_load_time:.3f} s", f"{round(rows / max(db_load_time, 0.0001), 1):,} rows/s"],
        ["Metrics Partition Aggregation", f"{agg_time:.3f} s", f"{round(len(affected_dates) / max(agg_time, 0.0001), 1):,} dates/s"],
        ["TOTAL PIPELINE TIME", f"{total_pipeline_time:.3f} s", f"{throughput_rps:,} events/s"]
    ]

    print("\nBenchmark Breakdown:")
    print(tabulate(perf_table, headers=["Pipeline Stage", "Duration", "Throughput"], tablefmt="grid"))
    print(f"\nMemory Footprint: Initial={mem_start} MB | Peak={mem_end} MB | Delta=+{mem_delta} MB")

    # Clean up temp file
    if temp_csv.exists():
        temp_csv.unlink()

    return {
        "rows": rows,
        "total_time": total_pipeline_time,
        "throughput_rps": throughput_rps,
        "mem_delta_mb": mem_delta
    }


def main():
    parser = argparse.ArgumentParser(description="Pipeline Performance Benchmark Suite")
    parser.add_argument("--rows", type=int, default=10000, help="Total events to benchmark")
    args = parser.parse_args()
    run_benchmark(rows=args.rows)


if __name__ == "__main__":
    main()
