# Comprehensive Automated Testing Strategy

## 1. Test Architecture and Philosophy

The pipeline testing suite is engineered using `pytest` to guarantee deterministic, isolated, and repeatable test runs without requiring network access, external services, or pre-configured database daemons.

### Core Testing Pillars:
1. **Total Isolation**: Each test executes against a pristine, temporary in-memory/file-backed SQLite engine (`tmp_path` fixture) with foreign key enforcement active.
2. **True Business Validation**: Assertions test exact financial values, row counts, and state transitions—never superficial function execution.
3. **Exhaustive Edge Cases**: Validates every possible failure mode: corrupted types, negative decimals, missing foreign keys, late arrivals, out-of-order events, and version collisions.

---

## 2. Test Matrix and Requirement Traceability

| ID | Test Name | Module | Category | Description | Status |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | `test_valid_input_ingestion` | `test_ingestion.py` | Ingestion | Valid CSV read, parsed, and staged. | PASSED |
| 2 | `test_empty_input_file` | `test_ingestion.py` | Ingestion | 0-byte file handled safely without crashing. | PASSED |
| 3 | `test_missing_required_column` | `test_ingestion.py` | Ingestion | Missing column raises descriptive `IngestionError`.| PASSED |
| 4 | `test_missing_required_fields_rejected` | `test_validation.py` | Validation | Empty `customer_id`, `order_id`, `event_id` sent to quarantine.| PASSED |
| 5 | `test_malformed_records` | `test_validation.py` | Validation | Non-numeric quantities/prices and bad dates quarantined. | PASSED |
| 6 | `test_duplicate_event_id_handling` | `test_validation.py` | Validation | Duplicate `event_id` in batch/database detected & quarantined. | PASSED |
| 7 | `test_duplicate_file_ingestion` | `test_ingestion.py` | Ingestion | SHA-256 hash registry detects duplicate file & skips. | PASSED |
| 8 | `test_unexpected_schema_extra_columns` | `test_ingestion.py` | Ingestion | Unknown columns preserved in raw staging safely. | PASSED |
| 9 | `test_null_values` | `test_validation.py` | Validation | Nulls in critical business fields rejected. | PASSED |
| 10 | `test_invalid_negative_amount` | `test_validation.py` | Validation | Negative prices rejected to quarantine. | PASSED |
| 11 | `test_invalid_quantity` | `test_validation.py` | Validation | Zero or negative quantities rejected to quarantine. | PASSED |
| 12 | `test_invalid_reference_foreign_keys` | `test_validation.py` | Validation | Missing `customer_id` or `product_id` FKs rejected. | PASSED |
| 13 | `test_create_event_establishes_order` | `test_event_processing.py` | State Fold | `CREATE` establishes active order state. | PASSED |
| 14 | `test_update_event_modifies_order` | `test_event_processing.py` | State Fold | `UPDATE` modifies quantity and price in place. | PASSED |
| 15 | `test_cancel_event_marks_order_cancelled` | `test_event_processing.py` | State Fold | `CANCEL` zeros out recognized order sales. | PASSED |
| 16 | `test_reversal_event_marks_order_reversed` | `test_event_processing.py` | State Fold | `REVERSAL` transitions order to `REVERSED`. | PASSED |
| 17 | `test_late_arriving_event_recomputes_historical_metrics` | `test_late_events.py` | Aggregation | Past-dated event triggers recalculation of target date. | PASSED |
| 18 | `test_out_of_order_events` | `test_event_processing.py` | State Fold | Out-of-order `UPDATE` before `CREATE` folds correctly. | PASSED |
| 19 | `test_conflicting_source_versions_older_cannot_overwrite_newer` | `test_event_processing.py` | State Fold | Version 1 cannot overwrite higher Version 2 state. | PASSED |
| 20 | `test_repeated_pipeline_execution_idempotency` | `test_idempotency.py` | Idempotency | Rerunning identical input causes zero duplicate rows. | PASSED |
| 21 | `test_partial_success_with_quarantine_routing` | `test_pipeline.py` | E2E | Valids applied, invalids quarantined (`PARTIAL_SUCCESS`).| PASSED |
| 22 | `test_retry_after_failure` | `test_pipeline.py` | Recovery | Retrying after partial failures safely completes. | PASSED |
| 23 | `test_backfill_recomputation_functionality` | `test_pipeline.py` | Backfill | Backfill over date range recomputes aggregate partitions. | PASSED |
| 24 | `test_cross_table_reconciliation_zero_discrepancy` | `test_reconciliation.py`| Reconciliation | Sales net == Sum(Product net) == Sum(Customer net). | PASSED |
| 25 | `test_starter_dataset_generation` | `test_data_generator.py`| Generator | Reproducible generation of 100-500 row starter dataset. | PASSED |
| 26 | `test_multiple_updates_in_single_batch` | `test_event_processing.py` | Edge Case | 3 sequential updates in single batch resolve to version 3. | PASSED |
| 27 | `test_aov_zero_when_no_completed_orders`| `test_metrics.py` | Edge Case | AOV is safely 0.00 when all orders on date are cancelled. | PASSED |
| 28 | `test_reversal_reduces_net_sales` | `test_reversals.py` | Edge Case | Reversal keeps gross sales, reduces net sales to 0.00. | PASSED |
| 29 | `test_generator_determinism_with_seed` | `test_data_generator.py` | Generator | Deterministic generation with identical random seed. | PASSED |
| 30 | `test_unknown_event_type_rejected` | `test_validation.py` | Validation | Unknown event actions rejected to quarantine. | PASSED |

---

## 3. Execution Command

To run the complete automated test suite:

```powershell
python -m pytest tests -v
# Or via CLI
python -m src.cli test
# Or standalone script
python scripts/run_tests.py
```
