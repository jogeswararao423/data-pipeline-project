"""
Unit and Integration Tests for Metrics Aggregation Module
"""

import pandas as pd
from sqlalchemy import text
from src.event_processor import EventProcessor
from src.metrics import MetricsAggregator


def test_daily_metrics_calculation(test_engine, sample_valid_row):
    """Verifies that daily sales, product, and customer metrics calculate exact financial totals."""
    processor = EventProcessor(test_engine, "sqlite")
    aggregator = MetricsAggregator(test_engine, "sqlite")

    # Order 1: Completed sale (qty=2, price=149.99, total=299.98)
    ord1 = dict(sample_valid_row, event_id="EVT-M1", order_id="ORD-M1")
    # Order 2: Cancelled order (qty=1, price=149.99)
    ord2_c = dict(sample_valid_row, event_id="EVT-M2-C", order_id="ORD-M2", source_version=1)
    ord2_can = dict(sample_valid_row, event_id="EVT-M2-X", order_id="ORD-M2", event_type="CANCEL", source_version=2)

    df = pd.DataFrame([ord1, ord2_c, ord2_can])
    processor.persist_processed_events(df)
    orders_ins, orders_upd, affected_dates = processor.update_order_states_for_batch(df)

    aggregator.recompute_daily_metrics_for_dates(affected_dates, run_id="METRIC-TEST")

    with test_engine.connect() as conn:
        sales = conn.execute(
            text("SELECT * FROM daily_sales_metrics WHERE business_date = :bdate"),
            {"bdate": sample_valid_row["business_date"]}
        ).mappings().first()

    assert sales is not None
    assert sales["total_orders"] == 2
    assert sales["completed_orders"] == 1
    assert sales["cancelled_orders"] == 1
    assert float(sales["net_sales"]) == 299.98
    assert float(sales["average_order_value"]) == 299.98
    assert sales["total_units_sold"] == 2
    assert sales["distinct_customers"] == 1


def test_aov_zero_when_no_completed_orders(test_engine, sample_valid_row):
    """Average Order Value (AOV) is safely 0.00 when all orders on a date are cancelled."""
    processor = EventProcessor(test_engine, "sqlite")
    aggregator = MetricsAggregator(test_engine, "sqlite")

    ord_c = dict(sample_valid_row, event_id="EVT-AOV-1", order_id="ORD-AOV", source_version=1)
    ord_x = dict(sample_valid_row, event_id="EVT-AOV-2", order_id="ORD-AOV", event_type="CANCEL", source_version=2)

    df = pd.DataFrame([ord_c, ord_x])
    processor.persist_processed_events(df)
    processor.update_order_states_for_batch(df)
    aggregator.recompute_daily_metrics_for_dates([sample_valid_row["business_date"]], run_id="AOV-TEST")

    with test_engine.connect() as conn:
        sales = conn.execute(
            text("SELECT * FROM daily_sales_metrics WHERE business_date = :bdate"),
            {"bdate": sample_valid_row["business_date"]}
        ).mappings().first()

    assert sales["completed_orders"] == 0
    assert sales["cancelled_orders"] == 1
    assert float(sales["net_sales"]) == 0.00
    assert float(sales["average_order_value"]) == 0.00
