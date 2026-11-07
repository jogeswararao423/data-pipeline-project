"""
Integration Tests for Cross-Table Reconciliation and Audit Balancing
"""

from pathlib import Path
import pandas as pd
from sqlalchemy import text
from src.pipeline import PipelineOrchestrator


def test_cross_table_reconciliation_zero_discrepancy(test_engine, tmp_path):
    """Test 24: Cross-table balancing: daily sales net total == sum(product metrics net) == sum(customer metrics net)."""
    orchestrator = PipelineOrchestrator(engine=test_engine, engine_type="sqlite")

    csv_file = tmp_path / "recon_input.csv"
    pd.DataFrame([
        {
            "event_id": "EVT-RC-01",
            "order_id": "ORD-RC-01",
            "customer_id": "CUST-1001",
            "product_id": "PROD-2001",
            "event_type": "CREATE",
            "quantity": 1,
            "unit_price": 149.99,
            "event_timestamp": "2026-01-04 10:00:00",
            "business_date": "2026-01-04",
            "source_file": "recon_input.csv",
            "source_version": 1
        },
        {
            "event_id": "EVT-RC-02",
            "order_id": "ORD-RC-02",
            "customer_id": "CUST-1002",
            "product_id": "PROD-2002",
            "event_type": "CREATE",
            "quantity": 3,
            "unit_price": 89.50,
            "event_timestamp": "2026-01-04 11:00:00",
            "business_date": "2026-01-04",
            "source_file": "recon_input.csv",
            "source_version": 1
        }
    ]).to_csv(csv_file, index=False)

    orchestrator.run(csv_file, force=True)

    with test_engine.connect() as conn:
        sales_net = conn.execute(text("SELECT COALESCE(SUM(net_sales), 0.00) FROM daily_sales_metrics")).scalar()
        prod_net = conn.execute(text("SELECT COALESCE(SUM(net_sales), 0.00) FROM daily_product_metrics")).scalar()
        cust_net = conn.execute(text("SELECT COALESCE(SUM(net_sales), 0.00) FROM daily_customer_metrics")).scalar()

    expected_total = round(149.99 + (3 * 89.50), 2)  # 149.99 + 268.50 = 418.49
    assert round(float(sales_net), 2) == expected_total
    assert round(float(prod_net), 2) == expected_total
    assert round(float(cust_net), 2) == expected_total
    assert abs(float(sales_net) - float(prod_net)) < 0.001
    assert abs(float(sales_net) - float(cust_net)) < 0.001
