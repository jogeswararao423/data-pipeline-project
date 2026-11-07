"""
Data Validation and Quality Assurance Module
Performs schema validation, data type verification, boundary checking,
referential integrity verification, and dead-letter quarantine routing.
"""

import json
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Tuple, List, Dict, Any, Set
import pandas as pd
from sqlalchemy import text, Engine

from src.config import Config

logger = logging.getLogger("ecommerce_pipeline.validation")

ALLOWED_EVENT_TYPES: Set[str] = {"CREATE", "UPDATE", "CANCEL", "REVERSAL"}


class DataValidator:
    """Validates raw transaction event streams against schema, domain, and relational rules."""

    def __init__(self, engine: Engine):
        self.engine = engine
        self._valid_customers: Set[str] = set()
        self._valid_products: Set[str] = set()
        self._existing_processed_events: Set[str] = set()
        self.refresh_reference_cache()

    def refresh_reference_cache(self) -> None:
        """Loads master reference keys from the database to validate foreign keys."""
        with self.engine.connect() as conn:
            cust_rows = conn.execute(text("SELECT customer_id FROM customers")).fetchall()
            self._valid_customers = {row[0] for row in cust_rows}

            prod_rows = conn.execute(text("SELECT product_id FROM products")).fetchall()
            self._valid_products = {row[0] for row in prod_rows}

            proc_rows = conn.execute(text("SELECT event_id FROM processed_transaction_events")).fetchall()
            self._existing_processed_events = {row[0] for row in proc_rows}

    def validate_records(
        self,
        df_raw: pd.DataFrame,
        run_id: str,
        source_file_name: str
    ) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, Any]]:
        """
        Splits raw records into valid_records and rejected_records.
        Returns:
            (df_valid, df_rejected, quality_metrics)
        """
        if df_raw.empty:
            empty_valid = pd.DataFrame(columns=[
                "event_id", "order_id", "customer_id", "product_id",
                "event_type", "quantity", "unit_price", "total_amount",
                "event_timestamp", "business_date", "source_file",
                "source_version", "run_id"
            ])
            empty_rejected = pd.DataFrame(columns=[
                "source_file", "source_row_number", "event_id",
                "rejection_reason", "rejected_payload", "run_id"
            ])
            return empty_valid, empty_rejected, {
                "total_records": 0, "valid_count": 0, "rejected_count": 0,
                "duplicates_count": 0, "fk_errors": 0, "boundary_errors": 0
            }

        valid_rows: List[Dict[str, Any]] = []
        rejected_rows: List[Dict[str, Any]] = []
        seen_batch_event_ids: Set[str] = set()

        duplicates_count = 0
        fk_errors = 0
        boundary_errors = 0
        missing_field_errors = 0

        records = df_raw.to_dict(orient="records")
        for idx, row_dict in enumerate(records):
            row_num = idx + 2  # 1-indexed accounting for CSV header row
            raw_payload_json = json.dumps({k: str(v) if pd.notna(v) else None for k, v in row_dict.items()})

            event_id = str(row_dict.get("event_id", "") or "").strip()
            order_id = str(row_dict.get("order_id", "") or "").strip()
            cust_id = str(row_dict.get("customer_id", "") or "").strip()
            prod_id = str(row_dict.get("product_id", "") or "").strip()
            event_type = str(row_dict.get("event_type", "") or "").strip().upper()
            raw_qty = row_dict.get("quantity")
            raw_price = row_dict.get("unit_price")
            raw_ts = str(row_dict.get("event_timestamp", "") or "").strip()
            raw_bdate = str(row_dict.get("business_date", "") or "").strip()
            raw_version = row_dict.get("source_version", 1)

            # 1. Null / Empty Required Identifiers
            if not event_id:
                rejected_rows.append({
                    "source_file": source_file_name,
                    "source_row_number": row_num,
                    "event_id": "MISSING_ID",
                    "rejection_reason": "MISSING_EVENT_ID: event_id is null or empty",
                    "rejected_payload": raw_payload_json,
                    "run_id": run_id
                })
                missing_field_errors += 1
                continue

            if not order_id:
                rejected_rows.append({
                    "source_file": source_file_name,
                    "source_row_number": row_num,
                    "event_id": event_id,
                    "rejection_reason": "MISSING_ORDER_ID: order_id is null or empty",
                    "rejected_payload": raw_payload_json,
                    "run_id": run_id
                })
                missing_field_errors += 1
                continue

            # 2. Duplicate Event ID Checks
            if event_id in seen_batch_event_ids:
                rejected_rows.append({
                    "source_file": source_file_name,
                    "source_row_number": row_num,
                    "event_id": event_id,
                    "rejection_reason": "DUPLICATE_EVENT_ID_BATCH: Event ID repeated within input batch",
                    "rejected_payload": raw_payload_json,
                    "run_id": run_id
                })
                duplicates_count += 1
                continue

            if event_id in self._existing_processed_events:
                rejected_rows.append({
                    "source_file": source_file_name,
                    "source_row_number": row_num,
                    "event_id": event_id,
                    "rejection_reason": "DUPLICATE_EVENT_ID_DATABASE: Event ID already exists in processed ledger",
                    "rejected_payload": raw_payload_json,
                    "run_id": run_id
                })
                duplicates_count += 1
                continue

            # 3. Event Type Verification
            if event_type not in ALLOWED_EVENT_TYPES:
                rejected_rows.append({
                    "source_file": source_file_name,
                    "source_row_number": row_num,
                    "event_id": event_id,
                    "rejection_reason": f"INVALID_EVENT_TYPE: '{event_type}' not in {sorted(list(ALLOWED_EVENT_TYPES))}",
                    "rejected_payload": raw_payload_json,
                    "run_id": run_id
                })
                boundary_errors += 1
                continue

            # 4. Customer and Product Missing / Referential Integrity
            if not cust_id:
                rejected_rows.append({
                    "source_file": source_file_name,
                    "source_row_number": row_num,
                    "event_id": event_id,
                    "rejection_reason": "MISSING_CUSTOMER_ID: customer_id is null or empty",
                    "rejected_payload": raw_payload_json,
                    "run_id": run_id
                })
                missing_field_errors += 1
                continue

            if self._valid_customers and cust_id not in self._valid_customers:
                rejected_rows.append({
                    "source_file": source_file_name,
                    "source_row_number": row_num,
                    "event_id": event_id,
                    "rejection_reason": f"FOREIGN_KEY_CUSTOMER_NOT_FOUND: customer_id '{cust_id}' does not exist in master customer dimension",
                    "rejected_payload": raw_payload_json,
                    "run_id": run_id
                })
                fk_errors += 1
                continue

            if not prod_id:
                rejected_rows.append({
                    "source_file": source_file_name,
                    "source_row_number": row_num,
                    "event_id": event_id,
                    "rejection_reason": "MISSING_PRODUCT_ID: product_id is null or empty",
                    "rejected_payload": raw_payload_json,
                    "run_id": run_id
                })
                missing_field_errors += 1
                continue

            if self._valid_products and prod_id not in self._valid_products:
                rejected_rows.append({
                    "source_file": source_file_name,
                    "source_row_number": row_num,
                    "event_id": event_id,
                    "rejection_reason": f"FOREIGN_KEY_PRODUCT_NOT_FOUND: product_id '{prod_id}' does not exist in master product dimension",
                    "rejected_payload": raw_payload_json,
                    "run_id": run_id
                })
                fk_errors += 1
                continue

            # 5. Quantity Validation
            try:
                qty = int(float(raw_qty))
                if qty <= 0:
                    rejected_rows.append({
                        "source_file": source_file_name,
                        "source_row_number": row_num,
                        "event_id": event_id,
                        "rejection_reason": f"INVALID_QUANTITY_NON_POSITIVE: quantity {qty} must be > 0",
                        "rejected_payload": raw_payload_json,
                        "run_id": run_id
                    })
                    boundary_errors += 1
                    continue
            except (ValueError, TypeError):
                rejected_rows.append({
                    "source_file": source_file_name,
                    "source_row_number": row_num,
                    "event_id": event_id,
                    "rejection_reason": f"MALFORMED_QUANTITY: '{raw_qty}' cannot be parsed as integer",
                    "rejected_payload": raw_payload_json,
                    "run_id": run_id
                })
                boundary_errors += 1
                continue

            # 6. Unit Price Validation
            try:
                price_dec = Decimal(str(raw_price)).quantize(Decimal("0.01"))
                if price_dec < Decimal("0.00"):
                    rejected_rows.append({
                        "source_file": source_file_name,
                        "source_row_number": row_num,
                        "event_id": event_id,
                        "rejection_reason": f"NEGATIVE_UNIT_PRICE: unit_price {price_dec} cannot be negative",
                        "rejected_payload": raw_payload_json,
                        "run_id": run_id
                    })
                    boundary_errors += 1
                    continue
            except (InvalidOperation, ValueError, TypeError):
                rejected_rows.append({
                    "source_file": source_file_name,
                    "source_row_number": row_num,
                    "event_id": event_id,
                    "rejection_reason": f"MALFORMED_UNIT_PRICE: '{raw_price}' is not a valid decimal number",
                    "rejected_payload": raw_payload_json,
                    "run_id": run_id
                })
                boundary_errors += 1
                continue

            # 7. Date and Timestamp Validation
            try:
                parsed_ts = pd.to_datetime(raw_ts)
                clean_ts_str = parsed_ts.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                rejected_rows.append({
                    "source_file": source_file_name,
                    "source_row_number": row_num,
                    "event_id": event_id,
                    "rejection_reason": f"INVALID_TIMESTAMP_FORMAT: '{raw_ts}' is not a valid datetime",
                    "rejected_payload": raw_payload_json,
                    "run_id": run_id
                })
                boundary_errors += 1
                continue

            try:
                parsed_bdate = pd.to_datetime(raw_bdate).date()
                clean_bdate_str = parsed_bdate.strftime("%Y-%m-%d")
            except Exception:
                rejected_rows.append({
                    "source_file": source_file_name,
                    "source_row_number": row_num,
                    "event_id": event_id,
                    "rejection_reason": f"INVALID_BUSINESS_DATE_FORMAT: '{raw_bdate}' is not a valid date",
                    "rejected_payload": raw_payload_json,
                    "run_id": run_id
                })
                boundary_errors += 1
                continue

            # 8. Source Version
            try:
                version = int(float(raw_version))
                if version < 1:
                    version = 1
            except Exception:
                version = 1

            # Total amount calculation with 2 decimal precision
            total_amt_dec = (price_dec * Decimal(str(qty))).quantize(Decimal("0.01"))

            # All checks passed!
            seen_batch_event_ids.add(event_id)
            valid_rows.append({
                "event_id": event_id,
                "order_id": order_id,
                "customer_id": cust_id,
                "product_id": prod_id,
                "event_type": event_type,
                "quantity": qty,
                "unit_price": float(price_dec),
                "total_amount": float(total_amt_dec),
                "event_timestamp": clean_ts_str,
                "business_date": clean_bdate_str,
                "source_file": source_file_name,
                "source_version": version,
                "run_id": run_id
            })

        df_valid = pd.DataFrame(valid_rows)
        df_rejected = pd.DataFrame(rejected_rows)

        summary = {
            "total_records": len(df_raw),
            "valid_count": len(df_valid),
            "rejected_count": len(df_rejected),
            "duplicates_count": duplicates_count,
            "fk_errors": fk_errors,
            "boundary_errors": boundary_errors,
            "missing_field_errors": missing_field_errors,
            "validation_rate_pct": round((len(df_valid) / len(df_raw) * 100.0), 2) if len(df_raw) > 0 else 100.0
        }

        # Persist rejected records
        if not df_rejected.empty:
            self.persist_rejected_records(df_rejected)

        return df_valid, df_rejected, summary

    def persist_rejected_records(self, df_rejected: pd.DataFrame) -> None:
        """Stores rejected records into database rejected_records table."""
        df_rejected.to_sql(
            "rejected_records",
            con=self.engine,
            if_exists="append",
            index=False,
            chunksize=Config.BATCH_SIZE
        )
        logger.info(f"Persisted {len(df_rejected)} rejected records to dead-letter quarantine table.")
