"""
Daily Metrics Aggregation Module
Computes sales, product, and customer metrics with partition-level upserts,
guaranteeing idempotency and accurate reversal/cancellation accounting.
"""

from decimal import Decimal
import logging
from typing import List, Dict, Any
from sqlalchemy import text, Engine

from src.config import Config

logger = logging.getLogger("ecommerce_pipeline.metrics")


class MetricsAggregator:
    """Calculates daily aggregates for sales, products, and customers."""

    def __init__(self, engine: Engine, engine_type: str):
        self.engine = engine
        self.engine_type = engine_type

    def recompute_daily_metrics_for_dates(self, business_dates: List[str], run_id: str) -> Dict[str, int]:
        """
        Recalculates daily sales, product, and customer metrics for the specified business dates.
        Uses safe partition-level upserts to ensure idempotency.
        """
        if not business_dates:
            return {"sales_metrics": 0, "product_metrics": 0, "customer_metrics": 0}

        sales_recomputed = 0
        product_recomputed = 0
        customer_recomputed = 0

        for bdate in sorted(business_dates):
            sm_count = self._recompute_sales_for_date(bdate, run_id)
            pm_count = self._recompute_products_for_date(bdate, run_id)
            cm_count = self._recompute_customers_for_date(bdate, run_id)

            sales_recomputed += sm_count
            product_recomputed += pm_count
            customer_recomputed += cm_count

        logger.info(
            f"Metrics recomputation complete for {len(business_dates)} dates: "
            f"Sales={sales_recomputed}, Products={product_recomputed}, Customers={customer_recomputed}."
        )

        return {
            "sales_metrics": sales_recomputed,
            "product_metrics": product_recomputed,
            "customer_metrics": customer_recomputed
        }

    def _recompute_sales_for_date(self, bdate: str, run_id: str) -> int:
        """Computes daily sales metrics for a single business date."""
        query = text("""
            SELECT 
                COUNT(*) AS total_orders,
                COUNT(CASE WHEN current_status IN ('CREATED', 'UPDATED') THEN 1 END) AS completed_orders,
                COUNT(CASE WHEN current_status = 'CANCELLED' THEN 1 END) AS cancelled_orders,
                COALESCE(SUM(CASE WHEN current_status IN ('CREATED', 'UPDATED', 'REVERSED') THEN (quantity * unit_price) ELSE 0 END), 0.00) AS gross_sales,
                COALESCE(SUM(CASE WHEN current_status IN ('CREATED', 'UPDATED') THEN total_amount ELSE 0.00 END), 0.00) AS net_sales,
                COALESCE(SUM(CASE WHEN current_status IN ('CREATED', 'UPDATED') THEN quantity ELSE 0 END), 0) AS total_units_sold,
                COUNT(DISTINCT CASE WHEN current_status IN ('CREATED', 'UPDATED') THEN customer_id END) AS distinct_customers
            FROM current_order_state
            WHERE business_date = :bdate
        """)

        with self.engine.connect() as conn:
            row = conn.execute(query, {"bdate": bdate}).mappings().first()

        total_orders = int(row["total_orders"] or 0)
        completed = int(row["completed_orders"] or 0)
        cancelled = int(row["cancelled_orders"] or 0)
        gross_sales = float(Decimal(str(row["gross_sales"] or 0)).quantize(Decimal("0.01")))
        net_sales = float(Decimal(str(row["net_sales"] or 0)).quantize(Decimal("0.01")))
        units_sold = int(row["total_units_sold"] or 0)
        distinct_cust = int(row["distinct_customers"] or 0)
        aov = round(net_sales / completed, 2) if completed > 0 else 0.00

        params = {
            "bdate": bdate,
            "total_orders": total_orders,
            "completed_orders": completed,
            "cancelled_orders": cancelled,
            "gross_sales": gross_sales,
            "net_sales": net_sales,
            "total_units_sold": units_sold,
            "distinct_customers": distinct_cust,
            "average_order_value": aov,
            "run_id": run_id
        }

        if self.engine_type == "mysql":
            upsert = text("""
                INSERT INTO daily_sales_metrics (
                    business_date, total_orders, completed_orders, cancelled_orders,
                    gross_sales, net_sales, total_units_sold, distinct_customers,
                    average_order_value, run_id
                ) VALUES (
                    :bdate, :total_orders, :completed_orders, :cancelled_orders,
                    :gross_sales, :net_sales, :total_units_sold, :distinct_customers,
                    :average_order_value, :run_id
                )
                ON DUPLICATE KEY UPDATE
                    total_orders = VALUES(total_orders),
                    completed_orders = VALUES(completed_orders),
                    cancelled_orders = VALUES(cancelled_orders),
                    gross_sales = VALUES(gross_sales),
                    net_sales = VALUES(net_sales),
                    total_units_sold = VALUES(total_units_sold),
                    distinct_customers = VALUES(distinct_customers),
                    average_order_value = VALUES(average_order_value),
                    last_updated_at = CURRENT_TIMESTAMP,
                    run_id = VALUES(run_id)
            """)
        else:
            upsert = text("""
                INSERT INTO daily_sales_metrics (
                    business_date, total_orders, completed_orders, cancelled_orders,
                    gross_sales, net_sales, total_units_sold, distinct_customers,
                    average_order_value, run_id
                ) VALUES (
                    :bdate, :total_orders, :completed_orders, :cancelled_orders,
                    :gross_sales, :net_sales, :total_units_sold, :distinct_customers,
                    :average_order_value, :run_id
                )
                ON CONFLICT(business_date) DO UPDATE SET
                    total_orders = excluded.total_orders,
                    completed_orders = excluded.completed_orders,
                    cancelled_orders = excluded.cancelled_orders,
                    gross_sales = excluded.gross_sales,
                    net_sales = excluded.net_sales,
                    total_units_sold = excluded.total_units_sold,
                    distinct_customers = excluded.distinct_customers,
                    average_order_value = excluded.average_order_value,
                    last_updated_at = CURRENT_TIMESTAMP,
                    run_id = excluded.run_id
            """)

        with self.engine.begin() as conn:
            conn.execute(upsert, params)

        return 1

    def _recompute_products_for_date(self, bdate: str, run_id: str) -> int:
        """Computes daily product aggregates for a single business date."""
        query = text("""
            SELECT 
                o.product_id,
                p.product_name,
                COALESCE(SUM(CASE WHEN o.current_status IN ('CREATED', 'UPDATED') THEN o.quantity ELSE 0 END), 0) AS units_sold,
                COALESCE(SUM(CASE WHEN o.current_status IN ('CREATED', 'UPDATED', 'REVERSED') THEN (o.quantity * o.unit_price) ELSE 0 END), 0.00) AS gross_sales,
                COALESCE(SUM(CASE WHEN o.current_status IN ('CREATED', 'UPDATED') THEN o.total_amount ELSE 0.00 END), 0.00) AS net_sales,
                COUNT(CASE WHEN o.current_status IN ('CREATED', 'UPDATED') THEN 1 END) AS order_count
            FROM current_order_state o
            JOIN products p ON o.product_id = p.product_id
            WHERE o.business_date = :bdate
            GROUP BY o.product_id, p.product_name
        """)

        with self.engine.connect() as conn:
            rows = conn.execute(query, {"bdate": bdate}).mappings().fetchall()

        if not rows:
            return 0

        if self.engine_type == "mysql":
            upsert = text("""
                INSERT INTO daily_product_metrics (
                    business_date, product_id, product_name,
                    units_sold, gross_sales, net_sales, order_count, run_id
                ) VALUES (
                    :bdate, :product_id, :product_name,
                    :units_sold, :gross_sales, :net_sales, :order_count, :run_id
                )
                ON DUPLICATE KEY UPDATE
                    product_name = VALUES(product_name),
                    units_sold = VALUES(units_sold),
                    gross_sales = VALUES(gross_sales),
                    net_sales = VALUES(net_sales),
                    order_count = VALUES(order_count),
                    last_updated_at = CURRENT_TIMESTAMP,
                    run_id = VALUES(run_id)
            """)
        else:
            upsert = text("""
                INSERT INTO daily_product_metrics (
                    business_date, product_id, product_name,
                    units_sold, gross_sales, net_sales, order_count, run_id
                ) VALUES (
                    :bdate, :product_id, :product_name,
                    :units_sold, :gross_sales, :net_sales, :order_count, :run_id
                )
                ON CONFLICT(business_date, product_id) DO UPDATE SET
                    product_name = excluded.product_name,
                    units_sold = excluded.units_sold,
                    gross_sales = excluded.gross_sales,
                    net_sales = excluded.net_sales,
                    order_count = excluded.order_count,
                    last_updated_at = CURRENT_TIMESTAMP,
                    run_id = excluded.run_id
            """)

        with self.engine.begin() as conn:
            for r in rows:
                p_gross = float(Decimal(str(r["gross_sales"] or 0)).quantize(Decimal("0.01")))
                p_net = float(Decimal(str(r["net_sales"] or 0)).quantize(Decimal("0.01")))
                conn.execute(upsert, {
                    "bdate": bdate,
                    "product_id": r["product_id"],
                    "product_name": r["product_name"],
                    "units_sold": int(r["units_sold"] or 0),
                    "gross_sales": p_gross,
                    "net_sales": p_net,
                    "order_count": int(r["order_count"] or 0),
                    "run_id": run_id
                })

        return len(rows)

    def _recompute_customers_for_date(self, bdate: str, run_id: str) -> int:
        """Computes daily customer aggregates for a single business date."""
        query = text("""
            SELECT 
                o.customer_id,
                COUNT(CASE WHEN o.current_status IN ('CREATED', 'UPDATED') THEN 1 END) AS order_count,
                COALESCE(SUM(CASE WHEN o.current_status IN ('CREATED', 'UPDATED') THEN o.quantity ELSE 0 END), 0) AS units_purchased,
                COALESCE(SUM(CASE WHEN o.current_status IN ('CREATED', 'UPDATED', 'REVERSED') THEN (o.quantity * o.unit_price) ELSE 0 END), 0.00) AS gross_sales,
                COALESCE(SUM(CASE WHEN o.current_status IN ('CREATED', 'UPDATED') THEN o.total_amount ELSE 0.00 END), 0.00) AS net_sales
            FROM current_order_state o
            WHERE o.business_date = :bdate
            GROUP BY o.customer_id
        """)

        with self.engine.connect() as conn:
            rows = conn.execute(query, {"bdate": bdate}).mappings().fetchall()

        if not rows:
            return 0

        if self.engine_type == "mysql":
            upsert = text("""
                INSERT INTO daily_customer_metrics (
                    business_date, customer_id, order_count,
                    units_purchased, gross_sales, net_sales, run_id
                ) VALUES (
                    :bdate, :customer_id, :order_count,
                    :units_purchased, :gross_sales, :net_sales, :run_id
                )
                ON DUPLICATE KEY UPDATE
                    order_count = VALUES(order_count),
                    units_purchased = VALUES(units_purchased),
                    gross_sales = VALUES(gross_sales),
                    net_sales = VALUES(net_sales),
                    last_updated_at = CURRENT_TIMESTAMP,
                    run_id = VALUES(run_id)
            """)
        else:
            upsert = text("""
                INSERT INTO daily_customer_metrics (
                    business_date, customer_id, order_count,
                    units_purchased, gross_sales, net_sales, run_id
                ) VALUES (
                    :bdate, :customer_id, :order_count,
                    :units_purchased, :gross_sales, :net_sales, :run_id
                )
                ON CONFLICT(business_date, customer_id) DO UPDATE SET
                    order_count = excluded.order_count,
                    units_purchased = excluded.units_purchased,
                    gross_sales = excluded.gross_sales,
                    net_sales = excluded.net_sales,
                    last_updated_at = CURRENT_TIMESTAMP,
                    run_id = excluded.run_id
            """)

        with self.engine.begin() as conn:
            for r in rows:
                c_gross = float(Decimal(str(r["gross_sales"] or 0)).quantize(Decimal("0.01")))
                c_net = float(Decimal(str(r["net_sales"] or 0)).quantize(Decimal("0.01")))
                conn.execute(upsert, {
                    "bdate": bdate,
                    "customer_id": r["customer_id"],
                    "order_count": int(r["order_count"] or 0),
                    "units_purchased": int(r["units_purchased"] or 0),
                    "gross_sales": c_gross,
                    "net_sales": c_net,
                    "run_id": run_id
                })

        return len(rows)
