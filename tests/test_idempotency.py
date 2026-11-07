"""
Integration Tests for Idempotency
Verifies that repeated executions of the pipeline or the same dataset
produce zero duplicate records and zero drift in state or aggregates.
"""

from pathlib import Path
import pandas as pd
from sqlalchemy import text
from src.pipeline import PipelineOrchestrator


def test_repeated_pipeline_execution_idempotency(test_engine, tmp_path):
    """Test 20: Rerunning the exact same dataset results in identical database counts and values."""
    csv_file = tmp_path / "idempotent_input.csv"
    df = pd.DataFrame([
        {
            "event_id": "EVT-IDEMP-01",
            "order_id": "ORD-IDEMP-01",
            "customer_id": "CUST-1001",
            "product_id": "PROD-2001",
            "event_type": "CREATE",
            "quantity": 2,
            "unit_price": 149.99,
            "event_timestamp": "2026-01-02 12:00:00",
            "business_date": "2026-01-02",
            "source_file": "idempotent_input.csv",
            "source_version": 1
        },
        {
            "event_id": "EVT-IDEMP-02",
            "order_id": "ORD-IDEMP-02",
            "customer_id": "CUST-1002",
            "product_id": "PROD-2002",
            "event_type": "CREATE",
            "quantity": 1,
            "unit_price": 89.50,
            "event_timestamp": "2026-01-02 13:00:00",
            "business_date": "2026-01-02",
            "source_file": "idempotent_input.csv",
            "source_version": 1
        }
    ])
    df.to_csv(csv_file, index=False)

    orchestrator = PipelineOrchestrator(engine=test_engine, engine_type="sqlite")

    # Run 1
    res1 = orchestrator.run(csv_file, force=True)
    assert res1["status"] == "SUCCESS"

    with test_engine.connect() as conn:
        events_1 = conn.execute(text("SELECT COUNT(*) FROM processed_transaction_events")).scalar()
        orders_1 = conn.execute(text("SELECT COUNT(*) FROM current_order_state")).scalar()
        sales_1 = conn.execute(text("SELECT net_sales FROM daily_sales_metrics WHERE business_date = '2026-01-02'")).scalar()

    # Run 2 with force=True (reprocessing same file)
    res2 = orchestrator.run(csv_file, force=True)

    with test_engine.connect() as conn:
        events_2 = conn.execute(text("SELECT COUNT(*) FROM processed_transaction_events")).scalar()
        orders_2 = conn.execute(text("SELECT COUNT(*) FROM current_order_state")).scalar()
        sales_2 = conn.execute(text("SELECT net_sales FROM daily_sales_metrics WHERE business_date = '2026-01-02'")).scalar()

    # Verify zero duplication
    assert events_1 == events_2 == 2
    assert orders_1 == orders_2 == 2
    assert float(sales_1) == float(sales_2) == 389.48
