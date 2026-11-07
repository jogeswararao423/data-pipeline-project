"""
Backfill and Historical Recomputation Module
Supports date-targeted and range-based aggregate recalculation,
recovery from upstream anomalies, and audit log generation.
"""

from datetime import datetime, timedelta
import logging
from typing import List, Dict, Any, Optional
import uuid
from sqlalchemy import text, Engine

from src.metrics import MetricsAggregator

logger = logging.getLogger("ecommerce_pipeline.backfill")


class BackfillManager:
    """Manages historical backfilling and idempotent recomputation of daily metrics."""

    def __init__(self, engine: Engine, engine_type: str):
        self.engine = engine
        self.engine_type = engine_type
        self.aggregator = MetricsAggregator(engine, engine_type)

    def get_dates_in_range(self, start_date: str, end_date: str) -> List[str]:
        """Generates list of continuous date strings between start and end dates inclusive."""
        dt_start = datetime.strptime(start_date, "%Y-%m-%d").date()
        dt_end = datetime.strptime(end_date, "%Y-%m-%d").date()
        if dt_start > dt_end:
            raise ValueError(f"start_date ({start_date}) cannot be after end_date ({end_date})")

        dates: List[str] = []
        cur = dt_start
        while cur <= dt_end:
            dates.append(cur.strftime("%Y-%m-%d"))
            cur += timedelta(days=1)
        return dates

    def run_backfill(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        specific_dates: Optional[List[str]] = None,
        run_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Executes backfill recomputation for the specified dates or range.
        """
        backfill_run_id = run_id or f"BACKFILL-{uuid.uuid4().hex[:8].upper()}"
        start_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        target_dates: List[str] = []
        if specific_dates:
            target_dates = sorted(list(set(specific_dates)))
        elif start_date and end_date:
            target_dates = self.get_dates_in_range(start_date, end_date)
        elif start_date:
            target_dates = [start_date]
        else:
            # Query all distinct dates present in current_order_state
            query = text("SELECT DISTINCT business_date FROM current_order_state ORDER BY business_date")
            with self.engine.connect() as conn:
                rows = conn.execute(query).fetchall()
                target_dates = [r[0] for r in rows]

        logger.info(f"Initiating backfill [{backfill_run_id}] for {len(target_dates)} dates: {target_dates}")

        # Log start in pipeline_run_log
        log_start_sql = text("""
            INSERT INTO pipeline_run_log (
                run_id, pipeline_stage, status, source_file,
                records_read, records_valid, records_rejected,
                records_processed, records_inserted, records_updated,
                metrics_recomputed, start_time
            ) VALUES (
                :run_id, 'BACKFILL', 'STARTED', 'N/A',
                0, 0, 0, 0, 0, 0, :dates_count, :start_time
            )
        """)
        with self.engine.begin() as conn:
            conn.execute(log_start_sql, {
                "run_id": backfill_run_id,
                "dates_count": len(target_dates),
                "start_time": start_time
            })

        try:
            results = self.aggregator.recompute_daily_metrics_for_dates(target_dates, backfill_run_id)
            end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            log_end_sql = text("""
                UPDATE pipeline_run_log SET
                    status = 'SUCCESS',
                    metrics_recomputed = :recomputed,
                    end_time = :end_time
                WHERE run_id = :run_id AND pipeline_stage = 'BACKFILL'
            """)
            with self.engine.begin() as conn:
                conn.execute(log_end_sql, {
                    "recomputed": results["sales_metrics"],
                    "end_time": end_time,
                    "run_id": backfill_run_id
                })

            return {
                "run_id": backfill_run_id,
                "status": "SUCCESS",
                "target_dates": target_dates,
                "results": results,
                "start_time": start_time,
                "end_time": end_time
            }

        except Exception as e:
            logger.error(f"Backfill {backfill_run_id} failed: {e}")
            end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_err_sql = text("""
                UPDATE pipeline_run_log SET
                    status = 'FAILED',
                    error_type = 'BACKFILL_ERROR',
                    error_message = :err,
                    end_time = :end_time
                WHERE run_id = :run_id AND pipeline_stage = 'BACKFILL'
            """)
            with self.engine.begin() as conn:
                conn.execute(log_err_sql, {
                    "err": str(e),
                    "end_time": end_time,
                    "run_id": backfill_run_id
                })
            raise e
