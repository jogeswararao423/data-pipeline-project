"""
Unit and Integration Tests for Validation and Quality Assurance Module
"""

import pandas as pd
from src.validation import DataValidator


def test_missing_required_fields_rejected(test_engine, sample_valid_row):
    """Test 4: Missing customer_id, order_id, or event_id rejected to quarantine."""
    validator = DataValidator(test_engine)
    
    row_no_cust = dict(sample_valid_row, event_id="EVT-NO-CUST", customer_id="")
    row_no_ord = dict(sample_valid_row, event_id="EVT-NO-ORD", order_id="")
    row_no_evt = dict(sample_valid_row, event_id="")

    df = pd.DataFrame([row_no_cust, row_no_ord, row_no_evt])
    df_valid, df_rej, summary = validator.validate_records(df, run_id="VAL-RUN", source_file_name="test.csv")

    assert len(df_valid) == 0
    assert len(df_rej) == 3
    assert summary["rejected_count"] == 3


def test_malformed_records(test_engine, sample_valid_row):
    """Test 5: Non-numeric quantity or price strings rejected."""
    validator = DataValidator(test_engine)

    row_bad_qty = dict(sample_valid_row, event_id="EVT-MAL-1", quantity="NOT_A_NUMBER")
    row_bad_price = dict(sample_valid_row, event_id="EVT-MAL-2", unit_price="INVALID_MONEY")
    row_bad_date = dict(sample_valid_row, event_id="EVT-MAL-3", event_timestamp="BAD_TIMESTAMP")

    df = pd.DataFrame([row_bad_qty, row_bad_price, row_bad_date])
    df_valid, df_rej, summary = validator.validate_records(df, run_id="VAL-RUN", source_file_name="test.csv")

    assert len(df_valid) == 0
    assert len(df_rej) == 3
    assert any("MALFORMED_QUANTITY" in r for r in df_rej["rejection_reason"])
    assert any("MALFORMED_UNIT_PRICE" in r for r in df_rej["rejection_reason"])
    assert any("INVALID_TIMESTAMP_FORMAT" in r for r in df_rej["rejection_reason"])


def test_duplicate_event_id_handling(test_engine, sample_valid_row):
    """Test 6: Duplicate event ID in batch and in database detected and quarantined."""
    validator = DataValidator(test_engine)

    # In-batch duplicate
    row1 = dict(sample_valid_row, event_id="EVT-DUP-1")
    row2 = dict(sample_valid_row, event_id="EVT-DUP-1")  # duplicate

    df = pd.DataFrame([row1, row2])
    df_valid, df_rej, summary = validator.validate_records(df, run_id="VAL-DUP", source_file_name="test.csv")

    assert len(df_valid) == 1
    assert len(df_rej) == 1
    assert "DUPLICATE_EVENT_ID_BATCH" in df_rej.iloc[0]["rejection_reason"]


def test_invalid_negative_amount(test_engine, sample_valid_row):
    """Test 10: Negative unit price rejected."""
    validator = DataValidator(test_engine)

    row_neg = dict(sample_valid_row, event_id="EVT-NEG-PRICE", unit_price=-19.99)
    df = pd.DataFrame([row_neg])
    df_valid, df_rej, summary = validator.validate_records(df, run_id="VAL-NEG", source_file_name="test.csv")

    assert len(df_valid) == 0
    assert len(df_rej) == 1
    assert "NEGATIVE_UNIT_PRICE" in df_rej.iloc[0]["rejection_reason"]


def test_invalid_quantity(test_engine, sample_valid_row):
    """Test 11: Zero or negative quantity rejected."""
    validator = DataValidator(test_engine)

    row_zero = dict(sample_valid_row, event_id="EVT-ZERO-QTY", quantity=0)
    row_neg = dict(sample_valid_row, event_id="EVT-NEG-QTY", quantity=-3)
    df = pd.DataFrame([row_zero, row_neg])
    df_valid, df_rej, summary = validator.validate_records(df, run_id="VAL-QTY", source_file_name="test.csv")

    assert len(df_valid) == 0
    assert len(df_rej) == 2
    assert all("INVALID_QUANTITY_NON_POSITIVE" in r for r in df_rej["rejection_reason"])


def test_invalid_reference_foreign_keys(test_engine, sample_valid_row):
    """Test 12: Missing customer or product references in master dimensions rejected."""
    validator = DataValidator(test_engine)

    row_bad_cust = dict(sample_valid_row, event_id="EVT-FK-1", customer_id="CUST-NONEXISTENT-999")
    row_bad_prod = dict(sample_valid_row, event_id="EVT-FK-2", product_id="PROD-UNKNOWN-888")
    df = pd.DataFrame([row_bad_cust, row_bad_prod])
    df_valid, df_rej, summary = validator.validate_records(df, run_id="VAL-FK", source_file_name="test.csv")

    assert len(df_valid) == 0
    assert len(df_rej) == 2
    assert any("FOREIGN_KEY_CUSTOMER_NOT_FOUND" in r for r in df_rej["rejection_reason"])
    assert any("FOREIGN_KEY_PRODUCT_NOT_FOUND" in r for r in df_rej["rejection_reason"])


def test_unknown_event_type_rejected(test_engine, sample_valid_row):
    """Unknown event type rejected."""
    validator = DataValidator(test_engine)

    row_unknown = dict(sample_valid_row, event_id="EVT-UNK", event_type="UNAUTHORIZED_COMMAND")
    df = pd.DataFrame([row_unknown])
    df_valid, df_rej, summary = validator.validate_records(df, run_id="VAL-UNK", source_file_name="test.csv")

    assert len(df_valid) == 0
    assert len(df_rej) == 1
    assert "INVALID_EVENT_TYPE" in df_rej.iloc[0]["rejection_reason"]
