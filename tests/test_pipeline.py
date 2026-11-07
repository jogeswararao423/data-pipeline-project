"""
End-to-End Pipeline Workflow and Recovery Tests
"""

from pathlib import Path
import pandas as pd
from sqlalchemy import text
from src.pipeline import PipelineOrchestrator
from src.backfill import BackfillManager


def test_partial_success_with_quarantine_routing(test_engine, tmp_path):
    """Test 21: Batch with both valid and invalid records routes invalids to quarantine while applying valids."""
    orchestrator = PipelineOrchestrator(engine=test_engine, engine_type="sqlite")

    csv_file = tmp_path / "mixed_input.csv"
    pd.DataFrame([
        # Valid row
        {
            "event_id": "EVT-OK-1",
            "order_id": "ORD-OK-1",
            "customer_id": "CUST-1001",
            "product_id": "PROD-2001",
            "event_type": "CREATE",
            "quantity": 1,
            "unit_price": 50.00,
            "event_timestamp": "2026-01-05 10:00:00",
            "business_date": "2026-01-05",
            "source_file": "mixed_input.csv",
            "source_version": 1
        },
        # Invalid row (missing customer)
        {
            "event_id": "EVT-BAD-1",
            "order_id": "ORD-BAD-1",
            "customer_id": "",
            "product_id": "PROD-2001",
            "event_type": "CREATE",
            "quantity": 1,
            "unit_price": 50.00,
            "event_timestamp": "2026-01-05 11:00:00",
            "business_date": "2026-01-05",
            "source_file": "mixed_input.csv",
            "source_version": 1
        }
    ]).to_csv(csv_file, index=False)

    res = orchestrator.run(csv_file, force=True)
    assert res["status"] == "PARTIAL_SUCCESS"
    assert res["records_read"] == 2
    assert res["records_valid"] == 1
    assert res["records_rejected"] == 1

    with test_engine.connect() as conn:
        valid_persisted = conn.execute(text("SELECT COUNT(*) FROM processed_transaction_events WHERE event_id = 'EVT-OK-1'")).scalar()
        rejected_persisted = conn.execute(text("SELECT COUNT(*) FROM rejected_records WHERE event_id = 'EVT-BAD-1'")).scalar()

    assert valid_persisted == 1
    assert rejected_persisted == 1


def test_backfill_recomputation_functionality(test_engine, tmp_path):
    """Test 23: Backfill recomputes historical date range accurately."""
    orchestrator = PipelineOrchestrator(engine=test_engine, engine_type="sqlite")

    csv_file = tmp_path / "bf_input.csv"
    pd.DataFrame([{
        "event_id": "EVT-BF-1",
        "order_id": "ORD-BF-1",
        "customer_id": "CUST-1001",
        "product_id": "PROD-2001",
        "event_type": "CREATE",
        "quantity": 2,
        "unit_price": 100.00,
        "event_timestamp": "2026-01-02 10:00:00",
        "business_date": "2026-01-02",
        "source_file": "bf_input.csv",
        "source_version": 1
    }]).to_csv(csv_file, index=False)

    orchestrator.run(csv_file, force=True)

    bf_mgr = BackfillManager(test_engine, "sqlite")
    bf_res = bf_mgr.run_backfill(start_date="2026-01-01", end_date="2026-01-03")

    assert bf_res["status"] == "SUCCESS"
    assert "2026-01-02" in bf_res["target_dates"]
    assert bf_res["results"]["sales_metrics"] == 3
