"""
Incremental Event Processor and Order State Reification Engine
Implements deterministic event sourcing, state folding, version precedence,
out-of-order event resolution, and idempotent state updates.
"""

from decimal import Decimal
import logging
from typing import List, Dict, Any, Tuple
import pandas as pd
from sqlalchemy import text, Engine

from src.config import Config

logger = logging.getLogger("ecommerce_pipeline.event_processor")


class EventProcessor:
    """Processes normalized transaction events, maintains immutable history, and updates order states."""

    def __init__(self, engine: Engine, engine_type: str):
        self.engine = engine
        self.engine_type = engine_type

    def persist_processed_events(self, df_valid: pd.DataFrame) -> int:
        """
        Idempotently inserts validated transaction events into the immutable ledger table.
        Skips any event_id that already exists in the ledger.
        """
        if df_valid.empty:
            return 0

        inserted_count = 0
        records = df_valid.to_dict(orient="records")
        # Ensure optional derived columns are populated
        for rec in records:
            if "total_amount" not in rec or pd.isna(rec["total_amount"]):
                p = Decimal(str(rec.get("unit_price", 0)))
                q = Decimal(str(rec.get("quantity", 0)))
                rec["total_amount"] = float((p * q).quantize(Decimal("0.01")))
            if "run_id" not in rec or pd.isna(rec["run_id"]):
                rec["run_id"] = "SYSTEM_RUN"
            if "source_file" not in rec or pd.isna(rec["source_file"]):
                rec["source_file"] = "SYSTEM"
            if "source_version" not in rec or pd.isna(rec["source_version"]):
                rec["source_version"] = 1

        if self.engine_type == "mysql":
            sql = text("""
                INSERT INTO processed_transaction_events (
                    event_id, order_id, customer_id, product_id,
                    event_type, quantity, unit_price, total_amount,
                    event_timestamp, business_date, source_file,
                    source_version, run_id
                ) VALUES (
                    :event_id, :order_id, :customer_id, :product_id,
                    :event_type, :quantity, :unit_price, :total_amount,
                    :event_timestamp, :business_date, :source_file,
                    :source_version, :run_id
                )
                ON DUPLICATE KEY UPDATE
                    event_id = event_id
            """)
        else:
            sql = text("""
                INSERT OR IGNORE INTO processed_transaction_events (
                    event_id, order_id, customer_id, product_id,
                    event_type, quantity, unit_price, total_amount,
                    event_timestamp, business_date, source_file,
                    source_version, run_id
                ) VALUES (
                    :event_id, :order_id, :customer_id, :product_id,
                    :event_type, :quantity, :unit_price, :total_amount,
                    :event_timestamp, :business_date, :source_file,
                    :source_version, :run_id
                )
            """)

        with self.engine.begin() as conn:
            for rec in records:
                res = conn.execute(sql, rec)
                if res.rowcount > 0:
                    inserted_count += 1

        logger.info(f"Persisted {inserted_count} new events to processed_transaction_events ledger.")
        return inserted_count

    def fold_order_state(self, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Folds an ordered sequence of events for a single order_id into its true current state.
        Sorting precedence: (source_version ASC, event_timestamp ASC, event_id ASC).
        """
        # Deterministic sorting
        sorted_events = sorted(
            events,
            key=lambda e: (
                int(e.get("source_version", 1)),
                str(e.get("event_timestamp", "")),
                str(e.get("event_id", ""))
            )
        )

        first_price = Decimal(str(sorted_events[0].get("unit_price", 0)))
        first_qty = int(sorted_events[0].get("quantity", 0))
        first_total = sorted_events[0].get("total_amount")
        if first_total is None or pd.isna(first_total):
            first_total = float((first_price * Decimal(str(first_qty))).quantize(Decimal("0.01")))
        else:
            first_total = float(Decimal(str(first_total)).quantize(Decimal("0.01")))

        state: Dict[str, Any] = {
            "order_id": sorted_events[0]["order_id"],
            "customer_id": sorted_events[0]["customer_id"],
            "product_id": sorted_events[0]["product_id"],
            "current_status": "CREATED",
            "quantity": first_qty,
            "unit_price": float(first_price.quantize(Decimal("0.01"))),
            "total_amount": first_total,
            "last_event_id": sorted_events[0]["event_id"],
            "last_event_type": sorted_events[0]["event_type"],
            "last_event_timestamp": sorted_events[0]["event_timestamp"],
            "source_version": int(sorted_events[0].get("source_version", 1)),
            "business_date": str(sorted_events[0]["business_date"])
        }

        for ev in sorted_events:
            ev_type = ev["event_type"].upper()
            ev_version = int(ev.get("source_version", 1))

            # Maintain lineage
            state["last_event_id"] = ev["event_id"]
            state["last_event_type"] = ev_type
            state["last_event_timestamp"] = ev["event_timestamp"]
            state["source_version"] = max(state["source_version"], ev_version)

            # Preserve or backfill business_date if first event was out of order
            if ev.get("business_date"):
                state["business_date"] = str(ev["business_date"])
            if ev.get("customer_id") and not state.get("customer_id"):
                state["customer_id"] = ev["customer_id"]
            if ev.get("product_id") and not state.get("product_id"):
                state["product_id"] = ev["product_id"]

            cur_p = Decimal(str(ev.get("unit_price", 0)))
            cur_q = int(ev.get("quantity", 0))
            cur_tot = ev.get("total_amount")
            if cur_tot is None or pd.isna(cur_tot):
                cur_tot = float((cur_p * Decimal(str(cur_q))).quantize(Decimal("0.01")))
            else:
                cur_tot = float(Decimal(str(cur_tot)).quantize(Decimal("0.01")))

            if ev_type == "CREATE":
                state["current_status"] = "CREATED"
                state["quantity"] = cur_q
                state["unit_price"] = float(cur_p.quantize(Decimal("0.01")))
                state["total_amount"] = cur_tot

            elif ev_type == "UPDATE":
                state["current_status"] = "UPDATED"
                state["quantity"] = cur_q
                state["unit_price"] = float(cur_p.quantize(Decimal("0.01")))
                state["total_amount"] = cur_tot

            elif ev_type == "CANCEL":
                state["current_status"] = "CANCELLED"
                # Cancelled orders zero out recognized sales
                state["total_amount"] = 0.00

            elif ev_type == "REVERSAL":
                state["current_status"] = "REVERSED"
                # Reversals zero out recognized net sales
                state["total_amount"] = 0.00

        return state

    def update_order_states_for_batch(self, df_valid: pd.DataFrame) -> Tuple[int, int, List[str]]:
        """
        Recomputes current order state for all order_ids in the current batch.
        Retrieves all historical events for these orders to ensure out-of-order
        and late events resolve deterministically.
        Returns:
            (orders_inserted, orders_updated, affected_business_dates)
        """
        if df_valid.empty:
            return 0, 0, []

        distinct_orders = list(df_valid["order_id"].unique())
        affected_dates_set = set(df_valid["business_date"].unique())

        # Retrieve all historical events for these orders from the ledger
        all_events_for_orders: List[Dict[str, Any]] = []

        chunk_size = 500
        for i in range(0, len(distinct_orders), chunk_size):
            chunk = distinct_orders[i:i + chunk_size]
            placeholders = ", ".join([f":ord_{idx}" for idx in range(len(chunk))])
            params = {f"ord_{idx}": oid for idx, oid in enumerate(chunk)}

            query = text(f"""
                SELECT event_id, order_id, customer_id, product_id, event_type,
                       quantity, unit_price, total_amount, event_timestamp,
                       business_date, source_file, source_version, run_id
                FROM processed_transaction_events
                WHERE order_id IN ({placeholders})
            """)

            with self.engine.connect() as conn:
                rows = conn.execute(query, params).mappings().fetchall()
                for r in rows:
                    all_events_for_orders.append(dict(r))

        # Group events by order_id
        events_by_order: Dict[str, List[Dict[str, Any]]] = {}
        for ev in all_events_for_orders:
            events_by_order.setdefault(ev["order_id"], []).append(ev)

        # Include any event in df_valid not yet in processed table if any
        for rec in df_valid.to_dict(orient="records"):
            oid = rec["order_id"]
            if oid in events_by_order:
                # Avoid duplicate event_id in memory fold
                existing_ids = {e["event_id"] for e in events_by_order[oid]}
                if rec["event_id"] not in existing_ids:
                    events_by_order[oid].append(rec)
            else:
                events_by_order[oid] = [rec]

        # Fold states
        folded_states: List[Dict[str, Any]] = []
        for oid, order_events in events_by_order.items():
            folded = self.fold_order_state(order_events)
            folded_states.append(folded)
            affected_dates_set.add(folded["business_date"])

        # Upsert states into current_order_state table
        orders_inserted = 0
        orders_updated = 0

        if self.engine_type == "mysql":
            upsert_sql = text("""
                INSERT INTO current_order_state (
                    order_id, customer_id, product_id, current_status,
                    quantity, unit_price, total_amount, last_event_id,
                    last_event_type, last_event_timestamp, source_version,
                    business_date
                ) VALUES (
                    :order_id, :customer_id, :product_id, :current_status,
                    :quantity, :unit_price, :total_amount, :last_event_id,
                    :last_event_type, :last_event_timestamp, :source_version,
                    :business_date
                )
                ON DUPLICATE KEY UPDATE
                    customer_id = VALUES(customer_id),
                    product_id = VALUES(product_id),
                    current_status = VALUES(current_status),
                    quantity = VALUES(quantity),
                    unit_price = VALUES(unit_price),
                    total_amount = VALUES(total_amount),
                    last_event_id = VALUES(last_event_id),
                    last_event_type = VALUES(last_event_type),
                    last_event_timestamp = VALUES(last_event_timestamp),
                    source_version = VALUES(source_version),
                    business_date = VALUES(business_date),
                    updated_at = CURRENT_TIMESTAMP
            """)
        else:
            upsert_sql = text("""
                INSERT INTO current_order_state (
                    order_id, customer_id, product_id, current_status,
                    quantity, unit_price, total_amount, last_event_id,
                    last_event_type, last_event_timestamp, source_version,
                    business_date
                ) VALUES (
                    :order_id, :customer_id, :product_id, :current_status,
                    :quantity, :unit_price, :total_amount, :last_event_id,
                    :last_event_type, :last_event_timestamp, :source_version,
                    :business_date
                )
                ON CONFLICT(order_id) DO UPDATE SET
                    customer_id = excluded.customer_id,
                    product_id = excluded.product_id,
                    current_status = excluded.current_status,
                    quantity = excluded.quantity,
                    unit_price = excluded.unit_price,
                    total_amount = excluded.total_amount,
                    last_event_id = excluded.last_event_id,
                    last_event_type = excluded.last_event_type,
                    last_event_timestamp = excluded.last_event_timestamp,
                    source_version = excluded.source_version,
                    business_date = excluded.business_date,
                    updated_at = CURRENT_TIMESTAMP
            """)

        with self.engine.begin() as conn:
            for s in folded_states:
                res = conn.execute(upsert_sql, s)
                if res.rowcount == 1:
                    orders_inserted += 1
                elif res.rowcount == 2:  # MySQL returns 2 on duplicate key update
                    orders_updated += 1
                else:
                    orders_updated += 1

        affected_business_dates = sorted(list(affected_dates_set))
        logger.info(
            f"Updated order states: {len(folded_states)} orders evaluated. "
            f"Affected business dates: {affected_business_dates}"
        )

        return orders_inserted, orders_updated, affected_business_dates
