"""
Integration Tests for Late-Arriving Events and Historical Recomputation
"""

from pathlib import Path
import pandas as pd
from sqlalchemy import text
from src.pipeline import PipelineOrchestrator


def test_late_arriving_event_recomputes_historical_metrics(test_engine, tmp_path):
    """Test 17: Late-arriving event for a past business date triggers correct historical recomputation."""
    orchestrator = PipelineOrchestrator(engine=test_engine, engine_type="sqlite")

    # Day 1: Regular batch for 2026-01-05
    file1 = tmp_path / "day1.csv"
    pd.DataFrame([{
        "event_id": "EVT-D1",
        "order_id": "ORD-D1",
        "customer_id": "CUST-1001",
        "product_id": "PROD-2001",
        "event_type": "CREATE",
        "quantity": 1,
        "unit_price": 100.00,
        "event_timestamp": "2026-01-05 10:00:00",
        "business_date": "2026-01-05",
        "source_file": "day1.csv",
        "source_version": 1
    }]).to_csv(file1, index=False)

    orchestrator.run(file1, force=True)

    with test_engine.connect() as conn:
        initial_net_sales = conn.execute(
            text("SELECT net_sales FROM daily_sales_metrics WHERE business_date = '2026-01-05'")
        ).scalar()
    assert float(initial_net_sales) == 100.00

    # Day 2: Ingested on 2026-01-07, but contains a late event for 2026-01-05
    file2 = tmp_path / "day2_late.csv"
    pd.DataFrame([{
        "event_id": "EVT-LATE-1",
        "order_id": "ORD-LATE-1",
        "customer_id": "CUST-1002",
        "product_id": "PROD-2002",
        "event_type": "CREATE",
        "quantity": 1,
        "unit_price": 50.00,
        "event_timestamp": "2026-01-05 23:45:00",  # Happened on Jan 5, arrived late
        "business_date": "2026-01-05",
        "source_file": "day2_late.csv",
        "source_version": 1
    }]).to_csv(file2, index=False)

    res2 = orchestrator.run(file2, force=True)
    assert "2026-01-05" in res2["affected_business_dates"]

    with test_engine.connect() as conn:
        recomputed_net_sales = conn.execute(
            text("SELECT net_sales FROM daily_sales_metrics WHERE business_date = '2026-01-05'")
        ).scalar()
        orders_count = conn.execute(
            text("SELECT total_orders FROM daily_sales_metrics WHERE business_date = '2026-01-05'")
        ).scalar()

    # Net sales should now accurately reflect 100 + 50 = 150
    assert float(recomputed_net_sales) == 150.00
    assert orders_count == 2
