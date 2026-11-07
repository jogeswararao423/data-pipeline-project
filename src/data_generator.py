"""
Synthetic Dataset Generator Module
Generates fully reproducible, edge-case-rich e-commerce transaction datasets
for small sample sets (100-500 rows) up to enterprise scale (1M+ rows).
"""

import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
import pandas as pd

from src.config import Config


# Default Known Reference Customers and Products (matching seed_data.sql)
KNOWN_CUSTOMERS = [f"CUST-{1001 + i}" for i in range(10)]
KNOWN_PRODUCTS = [
    ("PROD-2001", 149.99),
    ("PROD-2002", 89.50),
    ("PROD-2003", 49.95),
    ("PROD-2004", 329.00),
    ("PROD-2005", 59.99),
    ("PROD-2006", 24.50),
    ("PROD-2007", 119.00),
    ("PROD-2008", 29.99),
    ("PROD-2009", 34.00),
    ("PROD-2010", 39.95)
]


class SyntheticDataGenerator:
    """Configurable synthetic e-commerce event stream generator."""

    def __init__(
        self,
        seed: int = 42,
        customer_count: int = 10,
        product_count: int = 10,
        start_date: str = "2026-01-01",
        days_span: int = 7
    ):
        self.seed = seed
        self.customer_count = max(customer_count, len(KNOWN_CUSTOMERS))
        self.product_count = max(product_count, len(KNOWN_PRODUCTS))
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d")
        self.days_span = days_span
        
        # Seed RNGs for determinism
        random.seed(self.seed)
        
        # Build Customers list
        self.customers = list(KNOWN_CUSTOMERS)
        for i in range(11, self.customer_count + 1):
            self.customers.append(f"CUST-{1000 + i}")
            
        # Build Products list (id, unit_price)
        self.products = list(KNOWN_PRODUCTS)
        for i in range(11, self.product_count + 1):
            self.products.append((f"PROD-{2000 + i}", round(random.uniform(10.0, 500.0), 2)))

    def generate_events(
        self,
        total_rows: int = 300,
        invalid_pct: float = 0.05,
        duplicate_pct: float = 0.03,
        late_pct: float = 0.05,
        source_file_name: str = "transactions.csv",
        prefix: Optional[str] = None
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Generates transaction events with controlled percentages of updates,
        cancellations, reversals, duplicates, out-of-order events, and invalid payloads.
        """
        random.seed(self.seed)
        events: List[Dict[str, Any]] = []
        p_tag = f"{prefix}-" if prefix else ""
        
        # Track orders created to generate subsequent lifecycle events
        created_orders: List[Dict[str, Any]] = []
        event_counter = 1
        order_counter = 1

        # Target valid base transactions count
        valid_target = int(total_rows * (1.0 - invalid_pct - duplicate_pct))

        for _ in range(valid_target):
            order_id = f"ORD-{p_tag}{order_counter:06d}"
            order_counter += 1
            
            cust_id = random.choice(self.customers)
            prod_id, base_price = random.choice(self.products)
            qty = random.randint(1, 5)
            
            # Timestamp & Business Date
            day_offset = random.randint(0, self.days_span - 1)
            event_dt = self.start_date + timedelta(
                days=day_offset,
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )
            b_date = event_dt.strftime("%Y-%m-%d")
            
            # Check if this should be flagged as late-arriving (date shifted into past)
            if random.random() < late_pct and day_offset > 2:
                # Arrives late by backdating business date
                b_date = (event_dt - timedelta(days=2)).strftime("%Y-%m-%d")

            event_id = f"EVT-{p_tag}{event_counter:07d}"
            event_counter += 1
            
            base_event = {
                "event_id": event_id,
                "order_id": order_id,
                "customer_id": cust_id,
                "product_id": prod_id,
                "event_type": "CREATE",
                "quantity": qty,
                "unit_price": base_price,
                "event_timestamp": event_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "business_date": b_date,
                "source_file": source_file_name,
                "source_version": 1
            }
            events.append(base_event)
            created_orders.append(base_event)

        # Generate lifecycle events: Updates, Cancellations, Reversals
        # About 15% updates, 8% cancels, 5% reversals from created orders
        lifecycle_events: List[Dict[str, Any]] = []
        for ord_info in created_orders:
            roll = random.random()
            dt = datetime.strptime(ord_info["event_timestamp"], "%Y-%m-%d %H:%M:%S")
            
            if roll < 0.15:  # UPDATE
                event_counter += 1
                new_qty = ord_info["quantity"] + random.choice([1, 2, -1])
                if new_qty <= 0:
                    new_qty = 1
                upd_dt = dt + timedelta(minutes=random.randint(5, 120))
                upd_event = {
                    "event_id": f"EVT-{p_tag}{event_counter:07d}",
                    "order_id": ord_info["order_id"],
                    "customer_id": ord_info["customer_id"],
                    "product_id": ord_info["product_id"],
                    "event_type": "UPDATE",
                    "quantity": new_qty,
                    "unit_price": ord_info["unit_price"],
                    "event_timestamp": upd_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "business_date": ord_info["business_date"],
                    "source_file": source_file_name,
                    "source_version": 2
                }
                lifecycle_events.append(upd_event)

            elif roll < 0.23:  # CANCEL
                event_counter += 1
                cancel_dt = dt + timedelta(minutes=random.randint(10, 240))
                cancel_event = {
                    "event_id": f"EVT-{p_tag}{event_counter:07d}",
                    "order_id": ord_info["order_id"],
                    "customer_id": ord_info["customer_id"],
                    "product_id": ord_info["product_id"],
                    "event_type": "CANCEL",
                    "quantity": ord_info["quantity"],
                    "unit_price": ord_info["unit_price"],
                    "event_timestamp": cancel_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "business_date": ord_info["business_date"],
                    "source_file": source_file_name,
                    "source_version": 2
                }
                lifecycle_events.append(cancel_event)

            elif roll < 0.28:  # REVERSAL
                event_counter += 1
                rev_dt = dt + timedelta(hours=random.randint(1, 48))
                rev_event = {
                    "event_id": f"EVT-{p_tag}{event_counter:07d}",
                    "order_id": ord_info["order_id"],
                    "customer_id": ord_info["customer_id"],
                    "product_id": ord_info["product_id"],
                    "event_type": "REVERSAL",
                    "quantity": ord_info["quantity"],
                    "unit_price": ord_info["unit_price"],
                    "event_timestamp": rev_dt.strftime("%Y-%m-%d %H:%M:%S"),
                    "business_date": ord_info["business_date"],
                    "source_file": source_file_name,
                    "source_version": 3
                }
                lifecycle_events.append(rev_event)

        events.extend(lifecycle_events)

        # Inject Out-of-Order: Swap order of a few updates/cancels before their creates
        if len(lifecycle_events) >= 4:
            # Place two lifecycle events earlier in the list
            e1 = lifecycle_events[0]
            e2 = lifecycle_events[1]
            events.insert(0, e1)
            events.insert(2, e2)

        # Inject Duplicates (exact duplicate event IDs)
        dup_count = max(1, int(total_rows * duplicate_pct))
        for _ in range(dup_count):
            if events:
                dup_sample = dict(random.choice(events))
                events.append(dup_sample)

        # Inject Invalid Records
        invalid_count = max(1, int(total_rows * invalid_pct))
        invalid_types = [
            "missing_customer",
            "missing_product",
            "invalid_customer_fk",
            "invalid_product_fk",
            "negative_price",
            "zero_quantity",
            "unknown_event_type",
            "invalid_timestamp",
            "null_event_id"
        ]

        for i in range(invalid_count):
            event_counter += 1
            inv_kind = invalid_types[i % len(invalid_types)]
            base = {
                "event_id": f"EVT-INV-{p_tag}{event_counter:05d}",
                "order_id": f"ORD-INV-{p_tag}{event_counter:05d}",
                "customer_id": random.choice(self.customers),
                "product_id": self.products[0][0],
                "event_type": "CREATE",
                "quantity": 2,
                "unit_price": 49.99,
                "event_timestamp": "2026-01-03 14:00:00",
                "business_date": "2026-01-03",
                "source_file": source_file_name,
                "source_version": 1
            }

            if inv_kind == "missing_customer":
                base["customer_id"] = ""
            elif inv_kind == "missing_product":
                base["product_id"] = None
            elif inv_kind == "invalid_customer_fk":
                base["customer_id"] = "CUST-NONEXISTENT-999"
            elif inv_kind == "invalid_product_fk":
                base["product_id"] = "PROD-GHOST-888"
            elif inv_kind == "negative_price":
                base["unit_price"] = -25.50
            elif inv_kind == "zero_quantity":
                base["quantity"] = 0
            elif inv_kind == "unknown_event_type":
                base["event_type"] = "REFUND_SPECIAL_UNKNOWN"
            elif inv_kind == "invalid_timestamp":
                base["event_timestamp"] = "CORRUPT-DATE-XYZ"
            elif inv_kind == "null_event_id":
                base["event_id"] = ""

            events.append(base)

        # Shuffle slightly to emulate real async event streaming (while maintaining determinism via seed)
        random.shuffle(events)
        
        df = pd.DataFrame(events)

        metadata = {
            "total_generated": len(df),
            "valid_base_orders": len(created_orders),
            "lifecycle_events": len(lifecycle_events),
            "duplicates_injected": dup_count,
            "invalid_injected": invalid_count,
            "seed": self.seed
        }

        return df, metadata

    def save_to_csv(self, df: pd.DataFrame, output_path: Path) -> Path:
        """Saves generated dataset to CSV."""
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)
        return output_path


def generate_starter_dataset(
    output_path: Optional[Path] = None,
    seed: int = 42
) -> Tuple[Path, pd.DataFrame, Dict[str, Any]]:
    """Generates the official 100-500 row starter dataset for sample testing."""
    target_path = output_path or (Config.SAMPLE_DATA_DIR / "sample_transactions.csv")
    generator = SyntheticDataGenerator(seed=seed, days_span=7)
    df, meta = generator.generate_events(
        total_rows=250,
        invalid_pct=0.06,
        duplicate_pct=0.04,
        late_pct=0.08,
        source_file_name=target_path.name
    )
    generator.save_to_csv(df, target_path)
    return target_path, df, meta
