"""
Integration Tests for Financial Reversals and Chargebacks
"""

from pathlib import Path
import pandas as pd
from sqlalchemy import text
from src.pipeline import PipelineOrchestrator


def test_reversal_reduces_net_sales(test_engine, tmp_path):
    """Test 16 & Reversals: Reversal correctly reduces recognized net sales while preserving gross sales."""
    orchestrator = PipelineOrchestrator(engine=test_engine, engine_type="sqlite")

    # Initial Sale
    file1 = tmp_path / "sale.csv"
    pd.DataFrame([{
        "event_id": "EVT-SALE-1",
        "order_id": "ORD-REV-100",
        "customer_id": "CUST-1001",
        "product_id": "PROD-2001",
        "event_type": "CREATE",
        "quantity": 2,
        "unit_price": 149.99,
        "event_timestamp": "2026-01-03 10:00:00",
        "business_date": "2026-01-03",
        "source_file": "sale.csv",
        "source_version": 1
    }]).to_csv(file1, index=False)

    orchestrator.run(file1, force=True)

    with test_engine.connect() as conn:
        s1 = conn.execute(text("SELECT gross_sales, net_sales FROM daily_sales_metrics WHERE business_date = '2026-01-03'")).mappings().first()
    assert float(s1["gross_sales"]) == 299.98
    assert float(s1["net_sales"]) == 299.98

    # Reversal arrives next day for the Jan 3 sale
    file2 = tmp_path / "reversal.csv"
    pd.DataFrame([{
        "event_id": "EVT-REV-EVENT-1",
        "order_id": "ORD-REV-100",
        "customer_id": "CUST-1001",
        "product_id": "PROD-2001",
        "event_type": "REVERSAL",
        "quantity": 2,
        "unit_price": 149.99,
        "event_timestamp": "2026-01-04 09:00:00",
        "business_date": "2026-01-03",  # Affects Jan 3 financial balance
        "source_file": "reversal.csv",
        "source_version": 2
    }]).to_csv(file2, index=False)

    orchestrator.run(file2, force=True)

    with test_engine.connect() as conn:
        s2 = conn.execute(text("SELECT gross_sales, net_sales, completed_orders FROM daily_sales_metrics WHERE business_date = '2026-01-03'")).mappings().first()
        order_st = conn.execute(text("SELECT current_status, total_amount FROM current_order_state WHERE order_id = 'ORD-REV-100'")).mappings().first()

    assert order_st["current_status"] == "REVERSED"
    # Gross sales retains original transaction volume; net sales is reduced to 0.00
    assert float(s2["gross_sales"]) == 299.98
    assert float(s2["net_sales"]) == 0.00
    assert s2["completed_orders"] == 0
