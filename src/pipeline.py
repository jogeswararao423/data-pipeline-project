"""
End-to-End Pipeline Orchestration Engine
Coordinates ingestion, validation, state fold, metrics aggregation,
data quality monitoring, audit logging, and output artifact generation.
"""

from datetime import datetime
import logging
from pathlib import Path
import time
from typing import Dict, Any, Optional
import uuid
import pandas as pd
from sqlalchemy import text, Engine

from src.config import Config
from src.database import get_engine, init_database, seed_reference_data
from src.ingestion import FileIngestionManager
from src.validation import DataValidator
from src.transformation import normalize_event_dataframe
from src.event_processor import EventProcessor
from src.metrics import MetricsAggregator
from src.monitoring import DataQualityMonitor
from src.logging_config import setup_logger

logger = setup_logger("ecommerce_pipeline.orchestrator")


class PipelineOrchestrator:
    """End-to-end data engineering pipeline controller."""

    def __init__(self, engine: Optional[Engine] = None, engine_type: Optional[str] = None):
        if engine and engine_type:
            self.engine = engine
            self.engine_type = engine_type
        else:
            self.engine, self.engine_type = get_engine(use_fallback=True)

        # Initialize schema and seed data
        init_database(self.engine, self.engine_type)
        seed_reference_data(self.engine, self.engine_type)

        self.ingestion_manager = FileIngestionManager(self.engine, self.engine_type)
        self.validator = DataValidator(self.engine)
        self.processor = EventProcessor(self.engine, self.engine_type)
        self.aggregator = MetricsAggregator(self.engine, self.engine_type)
        self.monitor = DataQualityMonitor(self.engine)

    def generate_run_id(self) -> str:
        """Creates unique, traceable run identifier."""
        ts = datetime.now().strftime("%Y%m%d-%H%M%S")
        token = uuid.uuid4().hex[:6].upper()
        return f"RUN-{ts}-{token}"

    def run(self, input_file_path: Path, force: bool = False) -> Dict[str, Any]:
        """
        Executes full incremental pipeline workflow against target file.
        """
        run_id = self.generate_run_id()
        start_ts = time.time()
        start_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        logger.info(f"=== Starting Pipeline Run [{run_id}] for input: {input_file_path} ===")

        # Log initial run record
        init_log_sql = text("""
            INSERT INTO pipeline_run_log (
                run_id, pipeline_stage, status, source_file,
                records_read, records_valid, records_rejected,
                records_processed, records_inserted, records_updated,
                metrics_recomputed, start_time
            ) VALUES (
                :run_id, 'PIPELINE_TOTAL', 'STARTED', :file,
                0, 0, 0, 0, 0, 0, 0, :start_time
            )
        """)
        with self.engine.begin() as conn:
            conn.execute(init_log_sql, {
                "run_id": run_id,
                "file": str(input_file_path),
                "start_time": start_time_str
            })

        try:
            # Step 1: Ingestion
            logger.info("Executing Stage 1: Ingestion...")
            df_raw, ingest_meta = self.ingestion_manager.ingest_file(
                input_file_path,
                run_id=run_id,
                force=force
            )

            if ingest_meta.get("already_processed"):
                logger.info(f"File {input_file_path.name} was previously processed. Pipeline finished idempotently.")
                duration = round(time.time() - start_ts, 3)
                end_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                with self.engine.begin() as conn:
                    conn.execute(text("""
                        UPDATE pipeline_run_log SET
                            status = 'SKIPPED_ALREADY_PROCESSED',
                            duration_seconds = :duration,
                            end_time = :end_time
                        WHERE run_id = :run_id AND pipeline_stage = 'PIPELINE_TOTAL'
                    """), {"duration": duration, "end_time": end_time_str, "run_id": run_id})
                return {
                    "run_id": run_id,
                    "status": "SKIPPED_ALREADY_PROCESSED",
                    "records_read": 0,
                    "records_valid": 0,
                    "records_rejected": 0,
                    "duration_seconds": duration
                }

            if ingest_meta.get("is_empty"):
                logger.warning(f"File {input_file_path.name} is empty. Finishing with zero records.")
                duration = round(time.time() - start_ts, 3)
                end_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.ingestion_manager.update_file_status(ingest_meta["file_hash"], "SUCCESS")
                with self.engine.begin() as conn:
                    conn.execute(text("""
                        UPDATE pipeline_run_log SET
                            status = 'SUCCESS',
                            duration_seconds = :duration,
                            end_time = :end_time
                        WHERE run_id = :run_id AND pipeline_stage = 'PIPELINE_TOTAL'
                    """), {"duration": duration, "end_time": end_time_str, "run_id": run_id})
                return {
                    "run_id": run_id,
                    "status": "SUCCESS",
                    "records_read": 0,
                    "records_valid": 0,
                    "records_rejected": 0,
                    "duration_seconds": duration
                }

            # Step 2: Validation
            logger.info("Executing Stage 2: Validation & Referential Verification...")
            self.validator.refresh_reference_cache()
            df_valid, df_rejected, val_summary = self.validator.validate_records(
                df_raw,
                run_id=run_id,
                source_file_name=input_file_path.name
            )

            # Step 3: Transformation & Normalization
            logger.info("Executing Stage 3: Normalization & Precision Standardizations...")
            df_normalized = normalize_event_dataframe(df_valid)

            # Step 4: Event Processing & State Folding
            logger.info("Executing Stage 4: Event Sourcing Ledger & Current State Update...")
            events_persisted = self.processor.persist_processed_events(df_normalized)
            orders_ins, orders_upd, affected_dates = self.processor.update_order_states_for_batch(df_normalized)

            # Step 5: Daily Metrics Recalculation for Affected Business Dates
            logger.info(f"Executing Stage 5: Metrics Aggregation for dates: {affected_dates}...")
            metrics_results = self.aggregator.recompute_daily_metrics_for_dates(affected_dates, run_id)

            # Step 6: Automated Observability & Data Quality Verification
            logger.info("Executing Stage 6: Observability Health Checks...")
            dq_checks = self.monitor.run_all_checks(run_id)

            # Step 7: Finalize File Registry Status
            self.ingestion_manager.update_file_status(ingest_meta["file_hash"], "SUCCESS")

            # Step 8: Export Final CSV Deliverables
            logger.info("Executing Stage 7: Exporting Deliverables and Analytical Reports...")
            self.export_reports(run_id)

            # Step 9: Finalize Run Audit Log
            duration = round(time.time() - start_ts, 3)
            end_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            final_status = "PARTIAL_SUCCESS" if len(df_rejected) > 0 else "SUCCESS"

            with self.engine.begin() as conn:
                conn.execute(text("""
                    UPDATE pipeline_run_log SET
                        status = :status,
                        records_read = :read,
                        records_valid = :valid,
                        records_rejected = :rejected,
                        records_processed = :processed,
                        records_inserted = :inserted,
                        records_updated = :updated,
                        metrics_recomputed = :recomputed,
                        duration_seconds = :duration,
                        end_time = :end_time
                    WHERE run_id = :run_id AND pipeline_stage = 'PIPELINE_TOTAL'
                """), {
                    "status": final_status,
                    "read": len(df_raw),
                    "valid": len(df_valid),
                    "rejected": len(df_rejected),
                    "processed": events_persisted,
                    "inserted": orders_ins,
                    "updated": orders_upd,
                    "recomputed": metrics_results["sales_metrics"],
                    "duration": duration,
                    "end_time": end_time_str,
                    "run_id": run_id
                })

            summary = {
                "run_id": run_id,
                "status": final_status,
                "input_file": str(input_file_path),
                "records_read": len(df_raw),
                "records_valid": len(df_valid),
                "records_rejected": len(df_rejected),
                "events_persisted": events_persisted,
                "orders_inserted": orders_ins,
                "orders_updated": orders_upd,
                "affected_business_dates": affected_dates,
                "metrics_recomputed": metrics_results,
                "data_quality_checks": [c["status"] for c in dq_checks],
                "duration_seconds": duration
            }

            logger.info(f"=== Pipeline Run [{run_id}] Finished [{final_status}] in {duration}s ===")
            return summary

        except Exception as e:
            duration = round(time.time() - start_ts, 3)
            end_time_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logger.error(f"Pipeline Run [{run_id}] Encountered Fatal Error: {e}", exc_info=True)

            with self.engine.begin() as conn:
                conn.execute(text("""
                    UPDATE pipeline_run_log SET
                        status = 'FAILED',
                        error_type = 'UNHANDLED_EXCEPTION',
                        error_message = :err,
                        duration_seconds = :duration,
                        end_time = :end_time
                    WHERE run_id = :run_id AND pipeline_stage = 'PIPELINE_TOTAL'
                """), {
                    "err": str(e),
                    "duration": duration,
                    "end_time": end_time_str,
                    "run_id": run_id
                })
            raise e

    def export_reports(self, run_id: str) -> None:
        """Exports all required deliverables to CSV in outputs/reports directory."""
        Config.REPORTS_DIR.mkdir(parents=True, exist_ok=True)

        with self.engine.connect() as conn:
            # 1. Daily Sales Metrics
            df_sales = pd.read_sql("SELECT * FROM daily_sales_metrics ORDER BY business_date DESC", conn)
            df_sales.to_csv(Config.REPORTS_DIR / "daily_sales_metrics.csv", index=False)

            # 2. Daily Product Metrics
            df_prod = pd.read_sql("SELECT * FROM daily_product_metrics ORDER BY business_date DESC, net_sales DESC", conn)
            df_prod.to_csv(Config.REPORTS_DIR / "daily_product_metrics.csv", index=False)

            # 3. Daily Customer Metrics
            df_cust = pd.read_sql("SELECT * FROM daily_customer_metrics ORDER BY business_date DESC, net_sales DESC", conn)
            df_cust.to_csv(Config.REPORTS_DIR / "daily_customer_metrics.csv", index=False)

            # 4. Rejected Records
            df_rej = pd.read_sql("SELECT * FROM rejected_records ORDER BY reject_id DESC", conn)
            df_rej.to_csv(Config.REPORTS_DIR / "rejected_records.csv", index=False)

            # 5. Pipeline Run Summary
            df_log = pd.read_sql("SELECT * FROM pipeline_run_log ORDER BY start_time DESC", conn)
            df_log.to_csv(Config.REPORTS_DIR / "pipeline_run_summary.csv", index=False)

            # 6. Reconciliation Report
            recon_summary = [
                {"metric": "total_raw_events", "value": conn.execute(text("SELECT COUNT(*) FROM raw_transaction_events")).scalar() or 0},
                {"metric": "total_processed_events", "value": conn.execute(text("SELECT COUNT(*) FROM processed_transaction_events")).scalar() or 0},
                {"metric": "total_rejected_records", "value": conn.execute(text("SELECT COUNT(*) FROM rejected_records")).scalar() or 0},
                {"metric": "total_orders_in_current_state", "value": conn.execute(text("SELECT COUNT(*) FROM current_order_state")).scalar() or 0},
                {"metric": "total_daily_sales_net_amount", "value": conn.execute(text("SELECT COALESCE(SUM(net_sales), 0.00) FROM daily_sales_metrics")).scalar() or 0.0},
                {"metric": "total_product_sales_net_amount", "value": conn.execute(text("SELECT COALESCE(SUM(net_sales), 0.00) FROM daily_product_metrics")).scalar() or 0.0},
                {"metric": "total_customer_sales_net_amount", "value": conn.execute(text("SELECT COALESCE(SUM(net_sales), 0.00) FROM daily_customer_metrics")).scalar() or 0.0}
            ]
            pd.DataFrame(recon_summary).to_csv(Config.REPORTS_DIR / "reconciliation_report.csv", index=False)

        # 7. Render Interactive HTML Dashboard
        try:
            from src.dashboard_generator import generate_html_dashboard
            generate_html_dashboard(Config.REPORTS_DIR / "dashboard.html", engine=self.engine)
        except Exception as dash_err:
            logger.warning(f"Could not generate dashboard HTML: {dash_err}")

        logger.info("Successfully exported all deliverable CSV reports and interactive dashboard to outputs/reports/.")
