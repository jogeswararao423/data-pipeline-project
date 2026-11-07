"""
Unit and Integration Tests for Event Processing and State Folding
"""

import pandas as pd
from sqlalchemy import text
from src.event_processor import EventProcessor


def test_create_event_establishes_order(test_engine, sample_valid_row):
    """Test 13: CREATE event establishes active order state."""
    processor = EventProcessor(test_engine, "sqlite")
    df = pd.DataFrame([sample_valid_row])
    
    processor.persist_processed_events(df)
    orders_ins, orders_upd, affected_dates = processor.update_order_states_for_batch(df)

    with test_engine.connect() as conn:
        order = conn.execute(
            text("SELECT * FROM current_order_state WHERE order_id = :oid"),
            {"oid": sample_valid_row["order_id"]}
        ).mappings().first()

    assert order is not None
    assert order["current_status"] == "CREATED"
    assert order["quantity"] == 2
    assert float(order["unit_price"]) == 149.99
    assert float(order["total_amount"]) == 299.98


def test_update_event_modifies_order(test_engine, sample_valid_row):
    """Test 14: UPDATE event modifies quantity and recalculates total amount."""
    processor = EventProcessor(test_engine, "sqlite")
    
    evt_create = dict(sample_valid_row, source_version=1)
    evt_update = dict(sample_valid_row, event_id="EVT-UPD-01", event_type="UPDATE", quantity=4, total_amount=599.96, source_version=2)
    df = pd.DataFrame([evt_create, evt_update])

    processor.persist_processed_events(df)
    processor.update_order_states_for_batch(df)

    with test_engine.connect() as conn:
        order = conn.execute(
            text("SELECT * FROM current_order_state WHERE order_id = :oid"),
            {"oid": sample_valid_row["order_id"]}
        ).mappings().first()

    assert order["current_status"] == "UPDATED"
    assert order["quantity"] == 4
    assert float(order["total_amount"]) == 599.96
    assert order["source_version"] == 2


def test_cancel_event_marks_order_cancelled(test_engine, sample_valid_row):
    """Test 15: CANCEL event marks order cancelled with zero net sales."""
    processor = EventProcessor(test_engine, "sqlite")
    
    evt_create = dict(sample_valid_row, source_version=1)
    evt_cancel = dict(sample_valid_row, event_id="EVT-CAN-01", event_type="CANCEL", source_version=2)
    df = pd.DataFrame([evt_create, evt_cancel])

    processor.persist_processed_events(df)
    processor.update_order_states_for_batch(df)

    with test_engine.connect() as conn:
        order = conn.execute(
            text("SELECT * FROM current_order_state WHERE order_id = :oid"),
            {"oid": sample_valid_row["order_id"]}
        ).mappings().first()

    assert order["current_status"] == "CANCELLED"
    assert float(order["total_amount"]) == 0.00


def test_reversal_event_marks_order_reversed(test_engine, sample_valid_row):
    """Test 16: REVERSAL event marks order reversed with zero net sales."""
    processor = EventProcessor(test_engine, "sqlite")
    
    evt_create = dict(sample_valid_row, source_version=1)
    evt_rev = dict(sample_valid_row, event_id="EVT-REV-01", event_type="REVERSAL", source_version=2)
    df = pd.DataFrame([evt_create, evt_rev])

    processor.persist_processed_events(df)
    processor.update_order_states_for_batch(df)

    with test_engine.connect() as conn:
        order = conn.execute(
            text("SELECT * FROM current_order_state WHERE order_id = :oid"),
            {"oid": sample_valid_row["order_id"]}
        ).mappings().first()

    assert order["current_status"] == "REVERSED"
    assert float(order["total_amount"]) == 0.00


def test_out_of_order_events(test_engine, sample_valid_row):
    """Test 18: UPDATE arriving before CREATE resolves deterministically to the latest state."""
    processor = EventProcessor(test_engine, "sqlite")
    
    evt_create = dict(sample_valid_row, event_id="EVT-ORD-C", event_type="CREATE", quantity=2, source_version=1)
    evt_update = dict(sample_valid_row, event_id="EVT-ORD-U", event_type="UPDATE", quantity=5, total_amount=749.95, source_version=2)
    
    # Ingest UPDATE first
    df_first = pd.DataFrame([evt_update])
    processor.persist_processed_events(df_first)
    processor.update_order_states_for_batch(df_first)

    # Later CREATE arrives
    df_second = pd.DataFrame([evt_create])
    processor.persist_processed_events(df_second)
    processor.update_order_states_for_batch(df_second)

    with test_engine.connect() as conn:
        order = conn.execute(
            text("SELECT * FROM current_order_state WHERE order_id = :oid"),
            {"oid": sample_valid_row["order_id"]}
        ).mappings().first()

    # The higher version UPDATE still wins
    assert order["current_status"] == "UPDATED"
    assert order["quantity"] == 5
    assert float(order["total_amount"]) == 749.95


def test_conflicting_source_versions_older_cannot_overwrite_newer(test_engine, sample_valid_row):
    """Test 19: Older source_version cannot overwrite a newer state."""
    processor = EventProcessor(test_engine, "sqlite")

    evt_v2 = dict(sample_valid_row, event_id="EVT-V2", event_type="UPDATE", quantity=10, total_amount=1499.90, source_version=2)
    evt_v1 = dict(sample_valid_row, event_id="EVT-V1", event_type="UPDATE", quantity=1, total_amount=149.99, source_version=1)

    # Version 2 is applied first
    df_v2 = pd.DataFrame([evt_v2])
    processor.persist_processed_events(df_v2)
    processor.update_order_states_for_batch(df_v2)

    # Stale version 1 arrives later
    df_v1 = pd.DataFrame([evt_v1])
    processor.persist_processed_events(df_v1)
    processor.update_order_states_for_batch(df_v1)

    with test_engine.connect() as conn:
        order = conn.execute(
            text("SELECT * FROM current_order_state WHERE order_id = :oid"),
            {"oid": sample_valid_row["order_id"]}
        ).mappings().first()

    assert order["source_version"] == 2
    assert order["quantity"] == 10


def test_multiple_updates_in_single_batch(test_engine, sample_valid_row):
    """Domain Edge Case 26: Multiple updates to the same order within a single batch."""
    processor = EventProcessor(test_engine, "sqlite")

    evt_c = dict(sample_valid_row, source_version=1)
    evt_u1 = dict(sample_valid_row, event_id="EVT-U1", event_type="UPDATE", quantity=3, total_amount=449.97, source_version=2)
    evt_u2 = dict(sample_valid_row, event_id="EVT-U2", event_type="UPDATE", quantity=7, total_amount=1049.93, source_version=3)

    df_batch = pd.DataFrame([evt_c, evt_u1, evt_u2])
    processor.persist_processed_events(df_batch)
    processor.update_order_states_for_batch(df_batch)

    with test_engine.connect() as conn:
        order = conn.execute(
            text("SELECT * FROM current_order_state WHERE order_id = :oid"),
            {"oid": sample_valid_row["order_id"]}
        ).mappings().first()

    assert order["quantity"] == 7
    assert order["source_version"] == 3
    assert float(order["total_amount"]) == 1049.93
