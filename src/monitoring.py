"""
Automated Data Quality & Observability Monitoring Suite
Executes verifiable data quality rules, logs results to data_quality_results,
and surfaces data integrity metrics for pipeline runs.
"""

from datetime import datetime
import logging
from typing import List, Dict, Any
import pandas as pd
from sqlalchemy import text, Engine

from src.config import Config

logger = logging.getLogger("ecommerce_pipeline.monitoring")


class DataQualityMonitor:
    """Runs automated health checks and referential integrity verifications on pipeline tables."""

    def __init__(self, engine: Engine):
        self.engine = engine

    def run_all_checks(self, run_id: str) -> List[Dict[str, Any]]:
        """
        Executes comprehensive suite of data quality checks and persists results.
        """
        results: List[Dict[str, Any]] = []

        # 1. Event ID Uniqueness Check in Processed Ledger
        q_dup = text("""
            SELECT COUNT(*) AS dup_count 
            FROM (
                SELECT event_id 
                FROM processed_transaction_events 
                GROUP BY event_id 
                HAVING COUNT(*) > 1
            ) t
        """)
        with self.engine.connect() as conn:
            dup_events = conn.execute(q_dup).scalar() or 0
            total_events = conn.execute(text("SELECT COUNT(*) FROM processed_transaction_events")).scalar() or 0

        status_dup = "PASSED" if dup_events == 0 else "FAILED"
        results.append({
            "check_name": "EVENT_ID_UNIQUENESS",
            "table_name": "processed_transaction_events",
            "records_checked": total_events,
            "failed_records": dup_events,
            "status": status_dup,
            "details": f"Checked {total_events} events. Found {dup_events} duplicates."
        })

        # 2. Customer Referential Integrity Check
        q_cust_fk = text("""
            SELECT COUNT(*) 
            FROM processed_transaction_events p
            LEFT JOIN customers c ON p.customer_id = c.customer_id
            WHERE c.customer_id IS NULL
        """)
        with self.engine.connect() as conn:
            orphan_cust = conn.execute(q_cust_fk).scalar() or 0

        status_cust_fk = "PASSED" if orphan_cust == 0 else "FAILED"
        results.append({
            "check_name": "CUSTOMER_REFERENTIAL_INTEGRITY",
            "table_name": "processed_transaction_events",
            "records_checked": total_events,
            "failed_records": orphan_cust,
            "status": status_cust_fk,
            "details": f"Found {orphan_cust} events referencing missing customer_ids."
        })

        # 3. Product Referential Integrity Check
        q_prod_fk = text("""
            SELECT COUNT(*) 
            FROM processed_transaction_events p
            LEFT JOIN products pr ON p.product_id = pr.product_id
            WHERE pr.product_id IS NULL
        """)
        with self.engine.connect() as conn:
            orphan_prod = conn.execute(q_prod_fk).scalar() or 0

        status_prod_fk = "PASSED" if orphan_prod == 0 else "FAILED"
        results.append({
            "check_name": "PRODUCT_REFERENTIAL_INTEGRITY",
            "table_name": "processed_transaction_events",
            "records_checked": total_events,
            "failed_records": orphan_prod,
            "status": status_prod_fk,
            "details": f"Found {orphan_prod} events referencing missing product_ids."
        })

        # 4. Financial Non-Negative & Boundary Verification
        q_invalid_vals = text("""
            SELECT COUNT(*) 
            FROM processed_transaction_events 
            WHERE unit_price < 0 OR quantity <= 0 OR total_amount < 0
        """)
        with self.engine.connect() as conn:
            invalid_num = conn.execute(q_invalid_vals).scalar() or 0

        status_vals = "PASSED" if invalid_num == 0 else "FAILED"
        results.append({
            "check_name": "FINANCIAL_NON_NEGATIVE_AMOUNTS",
            "table_name": "processed_transaction_events",
            "records_checked": total_events,
            "failed_records": invalid_num,
            "status": status_vals,
            "details": f"Found {invalid_num} events with negative prices or zero quantities."
        })

        # 5. Daily Sales vs Product Metrics Reconciliation Check
        q_reconcile = text("""
            SELECT 
                COUNT(*) AS mismatch_count
            FROM (
                SELECT 
                    s.business_date,
                    s.net_sales AS sales_net,
                    COALESCE(SUM(p.net_sales), 0.00) AS prod_net,
                    ABS(s.net_sales - COALESCE(SUM(p.net_sales), 0.00)) AS diff
                FROM daily_sales_metrics s
                LEFT JOIN daily_product_metrics p ON s.business_date = p.business_date
                GROUP BY s.business_date, s.net_sales
                HAVING diff > 0.01
            ) t
        """)
        with self.engine.connect() as conn:
            dates_checked = conn.execute(text("SELECT COUNT(*) FROM daily_sales_metrics")).scalar() or 0
            mismatches = conn.execute(q_reconcile).scalar() or 0

        status_rec = "PASSED" if mismatches == 0 else "FAILED"
        results.append({
            "check_name": "METRICS_CROSS_TABLE_RECONCILIATION",
            "table_name": "daily_sales_metrics vs daily_product_metrics",
            "records_checked": dates_checked,
            "failed_records": mismatches,
            "status": status_rec,
            "details": f"Checked {dates_checked} dates. Net sales match products: {mismatches} discrepancies."
        })

        # Persist quality results
        insert_sql = text("""
            INSERT INTO data_quality_results (
                run_id, check_name, table_name, records_checked,
                failed_records, status, details
            ) VALUES (
                :run_id, :check_name, :table_name, :records_checked,
                :failed_records, :status, :details
            )
        """)
        with self.engine.begin() as conn:
            for r in results:
                conn.execute(insert_sql, {
                    "run_id": run_id,
                    "check_name": r["check_name"],
                    "table_name": r["table_name"],
                    "records_checked": r["records_checked"],
                    "failed_records": r["failed_records"],
                    "status": r["status"],
                    "details": r["details"]
                })

        logger.info(f"Executed {len(results)} automated data quality checks. All status: {[r['status'] for r in results]}")
        return results
