"""
Unit and Integration Tests for Ingestion Module
"""

from pathlib import Path
import pandas as pd
import pytest
from src.ingestion import FileIngestionManager, IngestionError


def test_valid_input_ingestion(test_engine, tmp_path):
    """Test 1: Valid input file successfully read and staged."""
    csv_file = tmp_path / "valid.csv"
    df = pd.DataFrame([{
        "event_id": "EVT-1",
        "order_id": "ORD-1",
        "customer_id": "CUST-1001",
        "product_id": "PROD-2001",
        "event_type": "CREATE",
        "quantity": 1,
        "unit_price": 50.00,
        "event_timestamp": "2026-01-01 10:00:00",
        "business_date": "2026-01-01"
    }])
    df.to_csv(csv_file, index=False)

    mgr = FileIngestionManager(test_engine, "sqlite")
    df_raw, meta = mgr.ingest_file(csv_file, run_id="TEST-RUN-1")

    assert len(df_raw) == 1
    assert meta["is_empty"] is False
    assert meta["already_processed"] is False
    assert meta["file_hash"] != "EMPTY_FILE"


def test_empty_input_file(test_engine, tmp_path):
    """Test 2: Empty 0-byte input file handled gracefully without exceptions."""
    empty_file = tmp_path / "empty.csv"
    empty_file.touch()

    mgr = FileIngestionManager(test_engine, "sqlite")
    df_raw, meta = mgr.ingest_file(empty_file, run_id="TEST-RUN-2")

    assert df_raw.empty
    assert meta["is_empty"] is True
    assert meta["row_count"] == 0


def test_missing_required_column(test_engine, tmp_path):
    """Test 3: Missing required column throws descriptive IngestionError."""
    bad_csv = tmp_path / "missing_col.csv"
    df = pd.DataFrame([{
        "event_id": "EVT-1",
        "order_id": "ORD-1",
        # Missing customer_id
        "product_id": "PROD-2001",
        "event_type": "CREATE",
        "quantity": 1,
        "unit_price": 50.00,
        "event_timestamp": "2026-01-01 10:00:00",
        "business_date": "2026-01-01"
    }])
    df.to_csv(bad_csv, index=False)

    mgr = FileIngestionManager(test_engine, "sqlite")
    with pytest.raises(IngestionError) as exc_info:
        mgr.ingest_file(bad_csv, run_id="TEST-RUN-3")

    assert "Missing required columns" in str(exc_info.value)
    assert "customer_id" in str(exc_info.value)


def test_duplicate_file_ingestion(test_engine, tmp_path):
    """Test 7: Ingesting the identical file twice is detected and safely skipped."""
    csv_file = tmp_path / "duplicate.csv"
    df = pd.DataFrame([{
        "event_id": "EVT-1",
        "order_id": "ORD-1",
        "customer_id": "CUST-1001",
        "product_id": "PROD-2001",
        "event_type": "CREATE",
        "quantity": 1,
        "unit_price": 50.00,
        "event_timestamp": "2026-01-01 10:00:00",
        "business_date": "2026-01-01"
    }])
    df.to_csv(csv_file, index=False)

    mgr = FileIngestionManager(test_engine, "sqlite")
    # First ingestion
    df1, meta1 = mgr.ingest_file(csv_file, run_id="RUN-1")
    mgr.update_file_status(meta1["file_hash"], "SUCCESS")
    assert len(df1) == 1

    # Second ingestion without force
    df2, meta2 = mgr.ingest_file(csv_file, run_id="RUN-2", force=False)
    assert df2.empty
    assert meta2["already_processed"] is True
    assert meta2["prior_run_id"] == "RUN-1"


def test_unexpected_schema_extra_columns(test_engine, tmp_path):
    """Test 8: Unexpected schema columns are handled gracefully and preserved."""
    csv_file = tmp_path / "extra_cols.csv"
    df = pd.DataFrame([{
        "event_id": "EVT-1",
        "order_id": "ORD-1",
        "customer_id": "CUST-1001",
        "product_id": "PROD-2001",
        "event_type": "CREATE",
        "quantity": 1,
        "unit_price": 50.00,
        "event_timestamp": "2026-01-01 10:00:00",
        "business_date": "2026-01-01",
        "extra_custom_metadata": "unplanned_field_123"
    }])
    df.to_csv(csv_file, index=False)

    mgr = FileIngestionManager(test_engine, "sqlite")
    df_raw, meta = mgr.ingest_file(csv_file, run_id="RUN-EXTRA")

    assert len(df_raw) == 1
    assert "extra_custom_metadata" in df_raw.columns
